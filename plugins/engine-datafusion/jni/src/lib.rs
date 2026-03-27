use std::cell::RefCell;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::num::NonZeroUsize;
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::ptr::addr_of_mut;
use jni::objects::{GlobalRef, JByteArray, JClass, JMap, JObject};
use jni::objects::JLongArray;
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
use jni::{JNIEnv, JavaVM};
use std::future::Future;
use std::sync::{Arc, OnceLock};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::{
    common::DataFusionError,
    datasource::listing::ListingTableUrl,
    execution::context::SessionContext,
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    execution::RecordBatchStream,
    prelude::*,
    DATAFUSION_VERSION,
};


use std::default::Default;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod util;
mod absolute_row_id_optimizer;
mod listing_table;
mod cache;
mod custom_cache_manager;
mod memory;
mod cross_rt_stream;
mod executor;
mod io;
mod runtime_manager;
mod cache_jni;
mod partial_agg_optimizer;
mod query_executor;
mod indexed_query_executor;
mod indexed_table;
mod project_row_id_analyzer;
pub mod logger;

// Import logger macros from shared crate
use vectorized_exec_spi::{log_info, log_error, log_debug};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use crate::custom_cache_manager::CustomCacheManager;
use crate::util::{create_file_meta_from_filenames, parse_string_arr, set_action_listener_error, set_action_listener_error_global, set_action_listener_ok, set_action_listener_ok_global, set_action_listener_ok_global_with_map};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};

use crate::statistics_cache::CustomStatisticsCache;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use std::result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{TryStreamExt, FutureExt};

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

// NativeBridge JNI implementations
use jni::objects::{JObjectArray, JString};
use log::error;
use once_cell::sync::Lazy;
use tokio_metrics::TaskMonitor;
use crate::cross_rt_stream::CrossRtStream;
use crate::memory::{Monitor, MonitoredMemoryPool};
use crate::runtime_manager::RuntimeManager;

mod statistics_cache;
mod eviction_policy;

struct DataFusionRuntime {
    runtime_env: RuntimeEnv,
    custom_cache_manager: Option<CustomCacheManager>,
    monitor: Arc<Monitor>,
}

// TASK monitorint metrics
static QUERY_EXECUTION_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(100)).clone()
});

static STREAM_NEXT_MONITOR: Lazy<TaskMonitor> = Lazy::new(|| {
    TaskMonitor::with_slow_poll_threshold(Duration::from_micros(50)).clone()
});

/// Per-query context stored in the ACTIVE_QUERIES registry.
/// Keyed by the ShardSearchContextId long (passed from Java as task_id).
/// Entries are inserted when executeQueryPhaseAsync begins and removed
/// when streamClose is called (or when the query phase errors before
/// returning a stream).
pub struct QueryContext {
    /// Token used to signal cancellation for this query.
    pub cancellation_token: CancellationToken,
    /// Handle for the cpu_executor task driving the CrossRtStream.
    /// Set after executeQueryPhaseAsync succeeds (the task is spawned eagerly inside
    /// CrossRtStream::new_with_df_error_stream). Calling abort() cancels the task at
    /// its next await point, freeing the cpu thread immediately without waiting for
    /// Java to call streamClose (which does the same via JoinSet drop).
    /// None until the stream is created, and None when the executor is shutting down.
    pub cpu_abort_handle: OnceLock<tokio::task::AbortHandle>,
}

impl QueryContext {
    fn new() -> Self {
        Self {
            cancellation_token: CancellationToken::new(),
            cpu_abort_handle: OnceLock::new(),
        }
    }
}

/// Registry of all in-flight queries on this node, keyed by ShardSearchContextId.
/// dashmap is already a declared dependency; tokio_util::sync::CancellationToken
/// is the only new addition.
static ACTIVE_QUERIES: Lazy<DashMap<i64, QueryContext>> = Lazy::new(DashMap::new);

// Global runtime manager
static TOKIO_RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();

// Global JavaVM reference
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

// Thread-local JNI environment. Each Tokio worker thread permanently attaches to the JVM
// on its first callback and reuses the same JNIEnv for all subsequent calls on that thread.
thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

// Helper function to get or attach JNI env
fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let mut env = jvm.attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");

            if let Some(name) = std::thread::current().name() {
                let _ = rename_current_jvm_thread(&mut env, name);
            }
            *opt = Some(env);
        }

        // Safe because we're the only one with access to this thread-local
        let env_ref = opt.as_mut().unwrap();
        f(env_ref)
    })
}

/// Rename the JVM thread to match the OS thread name so test thread-leak
/// filters can identify DataFusion threads. Best-effort; ignore failures.
/// jni-rs 0.22 provides a better way to do this (attach_current_thread_with_config),
/// but we're using jni-rs 0.21 and migration to 0.22 is not trivial.
fn rename_current_jvm_thread(env: &mut JNIEnv<'_>, name: &str) -> jni::errors::Result<()> {
    let thread = env
        .call_static_method("java/lang/Thread", "currentThread", "()Ljava/lang/Thread;", &[])?
        .l()?;
    let jname = env.new_string(name)?;
    env.call_method(&thread, "setName", "(Ljava/lang/String;)V", &[(&jname).into()])?;
    Ok(())
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Spawn an async task on `runtime` that calls an ActionListener exactly once.
///
/// The entire `task` future runs inside `catch_unwind`. Any panic is converted
/// to a `DataFusionError` and surfaced to the Java caller via `listener_ref`.
/// This ensures the `CompletableFuture` on the Java side is always completed,
/// never left hanging.
///
/// `on_ok` receives the success value and is responsible for calling the
/// appropriate `set_action_listener_ok_*` variant. `T` is inferred from
/// the closure, which in turn pins the `Output` type of `task`.
fn spawn_jni_task<Fut, T, FOk>(
    runtime: &tokio::runtime::Handle,
    task_name: &'static str,
    listener_ref: GlobalRef,
    task: Fut,
    on_ok: FOk,
)
where
    Fut: Future<Output = Result<T, DataFusionError>> + Send + 'static,
    T: Send + 'static,
    FOk: FnOnce(&mut JNIEnv, &GlobalRef, T) + Send + 'static,
{
    let _ = runtime.spawn(async move {
        let result = std::panic::AssertUnwindSafe(task)
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                let msg = panic_message(&panic);
                log_error!("{} panicked: {}", task_name, msg);
                Err(DataFusionError::Execution(format!("{} panicked: {}", task_name, msg)))
            });

        with_jni_env(|env| match result {
            Ok(value) => on_ok(env, &listener_ref, value),
            Err(e) => {
                log_error!("{} failed: {}", task_name, e);
                set_action_listener_error_global(env, &listener_ref, &e);
            }
        });
    });
}

/// Initialize the logger for Rust->Java logging bridge.
/// This should be called once when the native library is loaded.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger with the JVM for Rust->Java logging bridge
    // This uses the shared logger from vectorized_exec_spi
    // The logger stores its own JVM reference internally
    vectorized_exec_spi::logger::init_logger_from_env(&env);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    // Initialize JavaVM for async callbacks from Tokio worker threads
    // This is needed so worker threads can attach to JVM and call ActionListener methods
    JAVA_VM.get_or_init(|| {
        env.get_java_vm().expect("Failed to get JavaVM")
    });

    TOKIO_RUNTIME_MANAGER.get_or_init(|| {
        log_info!("Runtime manager initialized with {} CPU threads", cpu_threads);
        Arc::new(RuntimeManager::new(cpu_threads as usize))
    });
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    _env: JNIEnv,
    _class: JClass,
) {
    log_info!("Runtime manager shut down started");
    if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
        mgr.shutdown();
        log_info!("Runtime manager shut down successfully");
    }
}


#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_startTokioRuntimeMonitoring(
    _env: JNIEnv,
    _class: JClass,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Tokio runtime manager not initialized");
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    io_runtime.spawn(async move {
        let handle = tokio::runtime::Handle::current();
        let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
        log_info!("Tokio runtime monitoring started (interval: 5s)");
        for metrics in runtime_monitor.intervals() {
            log_runtime_metrics(&metrics);
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Log runtime metrics with performance analysis
#[allow(dead_code)]
fn log_runtime_metrics(metrics: &tokio_metrics::RuntimeMetrics) {
    log_info!("=== Runtime Metrics ===");
    log_info!("  Workers: {}", metrics.workers_count);
    log_info!("  Global queue depth: {}", metrics.global_queue_depth);
    log_info!("  Active queries (ACTIVE_QUERIES): {}", ACTIVE_QUERIES.len());
    /*
    //unstable tokio causes build failures, uncomment this when monitoring

    log_info!("  Worker overflow: {}", metrics.total_overflow_count);
    log_info!("  Remote schedule: {}", metrics.max_local_schedule_count);
    log_info!("  Worker steal ops: {}", metrics.total_steal_operations);
    log_info!("  Blocking queue depth: {}", metrics.blocking_queue_depth);
    log_info!("  Max local queue depth: {}", metrics.max_local_queue_depth);
    log_info!("  Min local queue depth: {}", metrics.min_local_queue_depth);
    log_info!("  Max local schedule count: {}", metrics.max_local_schedule_count);
    log_info!("  Min local schedule count: {}", metrics.min_local_schedule_count);
    log_info!("  Queue depth: {}", metrics.total_local_queue_depth);
    log_info!("  Total schedule count: {}", metrics.total_local_schedule_count);
    */
    let query_metrics = QUERY_EXECUTION_MONITOR.cumulative();
    log_task_metrics("Query exec (via CrossRtStream)", &query_metrics);
    let stream_metrics = STREAM_NEXT_MONITOR.cumulative();
    log_task_metrics("Stream Next (via CrossRtStream)", &stream_metrics);
    log_info!("======================");
}

/// Log task metrics with performance analysis
#[allow(dead_code)]
fn log_task_metrics(operation: &str, metrics: &tokio_metrics::TaskMetrics) {
    log_info!("=== Task Metrics: {} ===", operation);
    log_info!("  Scheduled duration: {:?}", metrics.total_scheduled_duration);
    log_info!("  Poll duration: {:?}", metrics.total_poll_duration);
    log_info!("  Idle duration: {:?}", metrics.total_idle_duration);
    log_info!("  Mean poll duration: {:?}", metrics.mean_poll_duration());
    log_info!("  Slow poll ratio: {:.2}%", metrics.slow_poll_ratio() * 100.0);
    log_info!("  Mean first poll delay: {:?}", metrics.mean_first_poll_delay());
    log_info!("  Total slow polls: {}", metrics.total_slow_poll_count);
    log_info!("  Total long delays: {}", metrics.total_long_delay_count);
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong
) -> jlong {
    let spill_dir: String = match env.get_string(&spill_dir) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let mut builder = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64);
    log_info!("Spill Limit is being set to : {}", spill_limit);
    let builder = builder.with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    let monitor = Arc::new(Monitor::default());
    let memory_pool = Arc::new(MonitoredMemoryPool::new(
        Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        )),
        monitor.clone(),
    ));

    let (cache_manager_config, custom_cache_manager) = match cache_manager_ptr {
        0 => {
            (CacheManagerConfig::default(), None)
        }
        _ => {
            let custom_cache_manager = unsafe { *Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
            (custom_cache_manager.build_cache_manager_config(), Some(custom_cache_manager))
        }
    };

    let runtime_env = RuntimeEnvBuilder::new()
        .with_cache_manager(cache_manager_config)
        .with_memory_pool(memory_pool.clone())
        .with_disk_manager_builder(builder)
        .build().unwrap();

    let runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager,
        monitor,
    };

    Box::into_raw(Box::new(runtime)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut DataFusionRuntime) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    if runtime_id == 0 {
        return 0;
    }
    let runtime_env = unsafe { &*(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtime_env.clone()));
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(
        r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#,
        DATAFUSION_VERSION
    );
    env.new_string(version_info)
        .expect("Couldn't create Java string")
        .as_raw()
}

/// Test JNI method to verify FFI boundary handling of sliced arrays.
/// Creates a sliced StringArray (simulating `head X from Y`) and returns FFI pointers.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createTestSlicedArray(
    mut env: JNIEnv,
    _class: JClass,
    offset: jint,
    length: jint,
    listener: JObject,
) {
    use arrow_schema::{Schema, Field, DataType};
    use arrow_array::StringArray;

    let original = StringArray::from(vec!["zero", "one", "two", "three", "four"]);
    let sliced = original.slice(offset as usize, length as usize);

    let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sliced)]).unwrap();

    let struct_array: StructArray = batch.into();
    let array_data = struct_array.to_data();

    let ffi_schema = FFI_ArrowSchema::try_from(array_data.data_type()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

    let ffi_array = FFI_ArrowArray::new(&array_data);
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;

    let result = env.new_long_array(2).unwrap();
    env.set_long_array_region(&result, 0, &[schema_ptr, array_ptr]).unwrap();

    let listener_class = env.get_object_class(&listener).unwrap();
    let on_response = env.get_method_id(&listener_class, "onResponse", "(Ljava/lang/Object;)V").unwrap();

    unsafe {
        env.call_method_unchecked(
            &listener,
            on_response,
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Void),
            &[jni::objects::JValue::Object(&result).as_jni()]
        ).unwrap();
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    let table_path: String = match env.get_string(&table_path) {
        Ok(path) => path.into(),
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid table path: {:?}", e),
            );
            return 0;
        }
    };

    let mut files: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(files) => files,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/IllegalArgumentException",
                format!("Invalid file list: {}", e),
            );
            return 0;
        }
    };

    // TODO: This works since files are named similarly ending with incremental generation count, preferably move this up to DatafusionReaderManager to keep file order
    files.sort();
    let files_metadata = match create_file_meta_from_filenames(&table_path, files.clone()) {
        Ok(metadata) => metadata,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create metadata: {}", err),
            );
            return 0;
        }
    };

    let table_url = match ListingTableUrl::parse(&table_path) {
        Ok(url) => url,
        Err(err) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Invalid table path: {}", err),
            );
            return 0;
        }
    };

    let shard_view = ShardView::new(table_url, files_metadata);

    Box::into_raw(Box::new(shard_view)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeDatafusionReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut ShardView) };
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_destroyTokioRuntime(
    mut env: JNIEnv,
    _class: JClass,
    tokio_runtime_ptr: jlong
)  {
    let _ = unsafe { Box::from_raw(tokio_runtime_ptr as *mut Runtime) };
}

pub struct ShardView {
    table_path: ListingTableUrl,
    files_metadata: Arc<Vec<CustomFileMeta>>,
}

impl ShardView {
    pub fn new(table_path: ListingTableUrl, files_metadata: Vec<CustomFileMeta>) -> Self {
        let files_metadata = Arc::new(files_metadata);
        ShardView {
            table_path,
            files_metadata,
        }
    }

    pub fn table_path(&self) -> ListingTableUrl {
        self.table_path.clone()
    }

    pub fn files_metadata(&self) -> Arc<Vec<CustomFileMeta>> {
        self.files_metadata.clone()
    }
}

#[derive(Debug, Clone)]
struct CustomFileMeta {
    row_group_row_counts: Arc<Vec<i64>>,
    row_base: Arc<i64>,
    object_meta: Arc<ObjectMeta>,
}

impl CustomFileMeta {
    pub fn new(row_group_row_counts: Vec<i64>, row_base: i64, object_meta: ObjectMeta) -> Self {
        let row_group_row_counts = Arc::new(row_group_row_counts);
        let row_base = Arc::new(row_base);
        let object_meta = Arc::new(object_meta);
        CustomFileMeta {
            row_group_row_counts,
            row_base,
            object_meta,
        }
    }

    pub fn row_group_row_counts(&self) -> Arc<Vec<i64>> {
        self.row_group_row_counts.clone()
    }

    pub fn row_base(&self) -> Arc<i64> {
        self.row_base.clone()
    }

    pub fn object_meta(&self) -> Arc<ObjectMeta> {
        self.object_meta.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStats {
    /// Total file size in bytes
    pub size: u64,

    /// Total number of rows in the file
    pub num_rows: i64,
}

impl FileStats {
    pub fn new(size: u64, num_rows: i64) -> Self {
        Self { size, num_rows }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn num_rows(&self) -> i64 {
        self.num_rows
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeQueryPhaseAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: jbyteArray,
    is_query_plan_explain_enabled: jboolean,
    target_partitions: jint,
    runtime_ptr: jlong,
    task_id: jlong,
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // ===== EXTRACT ALL JAVA DATA BEFORE ASYNC BLOCK =====
    let table_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("Failed to get table name: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let is_query_plan_explain_enabled: bool = is_query_plan_explain_enabled !=0;
    let target_partitions: usize = target_partitions as usize;

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            log_error!("Failed to convert plan bytes: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };
    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_meta = shard_view.files_metadata();

    // Register this query in the active-queries map before spawning, creating a fresh
    // CancellationToken. The token is raced against setup work via select! below so
    // that cancelQuery() can interrupt the query before a stream is returned to Java.
    let token = CancellationToken::new();
    ACTIVE_QUERIES.insert(task_id, QueryContext { cancellation_token: token.clone(), cpu_abort_handle: OnceLock::new() });

    spawn_jni_task(
        &io_runtime,
        "executeQueryPhaseAsync",
        listener_ref,
        async move {
            tokio::select! {
                result = query_executor::execute_query_with_cross_rt_stream(
                    table_path,
                    files_meta,
                    table_name,
                    plan_bytes_vec,
                    is_query_plan_explain_enabled,
                    target_partitions,
                    runtime,
                    cpu_executor,
                    task_id,
                ) => {
                    match result {
                        Ok((stream_ptr, abort_handle)) => {
                            // Store the cpu task's AbortHandle so cancelQuery() can abort it
                            // immediately without waiting for Java to call streamClose.
                            if let (Some(handle), Some(ctx)) =
                                (abort_handle, ACTIVE_QUERIES.get(&task_id))
                            {
                                let _ = ctx.cpu_abort_handle.set(handle);
                            }
                            Ok(stream_ptr)
                        }
                        Err(e) => {
                            // Query phase failed — no stream returned to Java, so
                            // streamClose will never be called; deregister here instead.
                            ACTIVE_QUERIES.remove(&task_id);
                            Err(e)
                        }
                    }
                }
                _ = token.cancelled() => {
                    ACTIVE_QUERIES.remove(&task_id);
                    Err(DataFusionError::Execution(format!("Query {} cancelled", task_id)))
                }
            }
        },
        |env, lr, ptr| set_action_listener_ok_global(env, lr, ptr),
    );
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_fetchSegmentStats(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_info!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Convert listener to GlobalRef (thread-safe)
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };
    let io_runtime = manager.io_runtime.clone();

    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let files_meta = shard_view.files_metadata();

    spawn_jni_task(
        &io_runtime,
        "fetchSegmentStats",
        listener_ref,
        async move { util::fetch_segment_statistics(files_meta).await },
        |env, lr, map| set_action_listener_ok_global_with_map(env, lr, &map),
    );
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    stream: jlong,
    task_id: jlong,
    listener: JObject,
) {
    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            set_action_listener_error(
                &mut env,
                listener,
                &DataFusionError::Execution("Runtime manager not initialized".to_string())
            );
            return;
        }
    };

    // Convert listener to GlobalRef
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                                    &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let stream_ptr = stream;
    let io_runtime = manager.io_runtime.clone();

    // Ensure stream_ptr lifetime is guaranteed beyond the spawn boundary
    // (e.g., wrap in Arc<Mutex<...>> or ensure sequential access contract)
    spawn_jni_task(
        &io_runtime,
        "streamNext",
        listener_ref,
        async move {
            let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
            // Look up the cancellation token and race try_next against it.
            // If the token fires, return EOF (0) immediately so Java's loadNextBatch()
            // completes with false and the batch loop exits naturally, after which
            // streamClose is called safely from the normal exit path.
            let token = ACTIVE_QUERIES.get(&task_id).map(|ctx| ctx.cancellation_token.clone());
            let maybe_batch = if let Some(token) = token {
                tokio::select! {
                    result = STREAM_NEXT_MONITOR.instrument(stream.try_next()) => result?,
                    _ = token.cancelled() => return Ok(0),
                }
            } else {
                STREAM_NEXT_MONITOR.instrument(stream.try_next()).await?
            };
            match maybe_batch {
                Some(batch) => {
                    log_info!("[RUST streamNext] Batch produced: {} rows, {} columns, schema: {:?}",
                        batch.num_rows(), batch.num_columns(), batch.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>());
                    // Convert to FFI
                    let struct_array: StructArray = batch.into();
                    let array_data = struct_array.into_data();
                    let ffi_array = FFI_ArrowArray::new(&array_data);
                    Ok(Box::into_raw(Box::new(ffi_array)) as jlong)
                }
                None => {
                    log_info!("[RUST streamNext] End of stream reached");
                    // end of stream
                    Ok(0)
                }
            }
        },
        |env, lr, ptr| set_action_listener_ok_global(env, lr, ptr),
    );
    // Function returns immediately to java - async rust work continues in background
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    listener: JObject,
) {
    if stream_ptr == 0 {
        set_action_listener_error(
            &mut env,
            listener,
            &DataFusionError::Execution("Invalid stream pointer".to_string())
        );
        return;
    }
    // Schema access is synchronous and fast - no need for runtime
    let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>) };
    //let stream = unsafe { &mut *(stream_ptr as *mut SendableRecordBatchStream) };

    let schema = stream.schema();
    match FFI_ArrowSchema::try_from(schema.as_ref()) {
        Ok(mut ffi_schema) => {
            set_action_listener_ok(&mut env, listener, addr_of_mut!(ffi_schema) as jlong);
        }
        Err(err) => {
            set_action_listener_error(&mut env, listener, &DataFusionError::Execution(
                format!("Schema conversion failed: {}", err)
            ));
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeFetchPhase(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    values: JLongArray,
    include_fields: JObjectArray,
    exclude_fields: JObjectArray,
    runtime_ptr: jlong,
    task_id: jlong,
    callback: JObject,
) -> jlong {
    let shard_view = unsafe { &*(shard_view_ptr as *const ShardView) };
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let table_path = shard_view.table_path();
    let files_metadata = shard_view.files_metadata();

    // Skip execution if this query was already cancelled before the fetch phase begins.
    if ACTIVE_QUERIES.get(&task_id).map_or(false, |ctx| ctx.cancellation_token.is_cancelled()) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Fetch phase skipped: query {} cancelled", task_id),
        );
        return 0;
    }

    let include_fields: Vec<String> =
        parse_string_arr(&mut env, include_fields).expect("Expected list of files");
    let exclude_fields: Vec<String> =
        parse_string_arr(&mut env, exclude_fields).expect("Expected list of files");

    // Safety checks first
    if values.is_null() {
        let _ = env.throw_new("java/lang/NullPointerException", "values array is null");
        return 0;
    }

    // Get array length
    let array_length = match env.get_array_length(&values) {
        Ok(len) => len,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array length: {:?}", e),
            );
            return 0;
        }
    };

    // Allocate Rust buffer
    let mut row_ids: Vec<jlong> = vec![0; array_length as usize];

    // Copy Java array into Rust buffer
    match env.get_long_array_region(values, 0, &mut row_ids[..]) {
        Ok(_) => {
            log_debug!("Received array: {:?}", row_ids);
        }
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to get array data: {:?}", e),
            );
            return 0;
        }
    }

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, callback,
                                    &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return 0;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();

    io_runtime.block_on(async {
        match query_executor::execute_fetch_phase(
            table_path,
            files_metadata,
            row_ids,
            include_fields,
            exclude_fields,
            runtime,
            cpu_executor,
            task_id,
        ).await {
            Ok(stream_ptr) => stream_ptr,
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Failed to execute fetch phase: {}", e),
                );
                0 // return 0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
    task_id: jlong,
) {
    // Deregister the query from the active-queries map. This is the normal
    // completion path (both end-of-stream and early close / cancellation).
    ACTIVE_QUERIES.remove(&task_id);
    let _ = unsafe { Box::from_raw(stream as *mut RecordBatchStreamAdapter<CrossRtStream>) };
}

/// Execute an indexed query asynchronously.
///
/// Registers an IndexedTableProvider under `tableName`, then executes the
/// substrait plan against it — same response path as executeQueryPhaseAsync.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_executeIndexedQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    weight_ptr: jlong,
    segment_max_docs: JLongArray,
    parquet_paths: JObjectArray,
    table_name: JString,
    substrait_bytes: jbyteArray,
    num_partitions: jint,
    bitset_mode: jint,
    is_query_plan_explain_enabled: jboolean,
    runtime_ptr: jlong,
    listener: JObject,
) {
    use crate::indexed_table::index::BitsetMode;

    let manager = match TOKIO_RUNTIME_MANAGER.get() {
        Some(m) => m,
        None => {
            log_error!("Runtime manager not initialized");
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution("Runtime manager not initialized".to_string()));
            return;
        }
    };

    // Extract all Java data before async block
    let seg_max_docs = {
        let len = match env.get_array_length(&segment_max_docs) {
            Ok(l) => l as usize,
            Err(e) => {
                set_action_listener_error(&mut env, listener,
                    &DataFusionError::Execution(format!("get_array_length: {}", e)));
                return;
            }
        };
        let mut buf = vec![0i64; len];
        if let Err(e) = env.get_long_array_region(segment_max_docs, 0, &mut buf) {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("get_long_array_region: {}", e)));
            return;
        }
        buf
    };

    let pq_paths = match parse_string_arr(&mut env, parquet_paths) {
        Ok(paths) => paths,
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("parse parquet paths: {}", e)));
            return;
        }
    };

    let tbl_name: String = match env.get_string(&table_name) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to get table name: {}", e)));
            return;
        }
    };

    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes) };
    let plan_bytes_vec = match env.convert_byte_array(plan_bytes_obj) {
        Ok(bytes) => bytes,
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };

    let n = (num_partitions as usize).max(1);
    let mode = match bitset_mode {
        1 => BitsetMode::Or,
        _ => BitsetMode::And,
    };

    let jvm = match JAVA_VM.get() {
        Some(vm) => Arc::new(unsafe {
            JavaVM::from_raw(vm.get_java_vm_pointer())
                .expect("Failed to create JavaVM from pointer")
        }),
        None => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution("JavaVM not initialized".to_string()));
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            log_error!("Failed to create global ref: {}", e);
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    // Pre-resolve the LuceneIndexSearcher class on the calling thread (which has the plugin classloader).
    // Tokio worker threads use the system classloader and can't find plugin classes.
    let searcher_class_ref = match env.find_class("org/opensearch/datafusion/search/LuceneIndexSearcher") {
        Ok(cls) => match env.new_global_ref(cls) {
            Ok(r) => r,
            Err(e) => {
                set_action_listener_error(&mut env, listener,
                    &DataFusionError::Execution(format!("Failed to create global ref for LuceneIndexSearcher: {}", e)));
                return;
            }
        },
        Err(e) => {
            set_action_listener_error(&mut env, listener,
                &DataFusionError::Execution(format!("Failed to find LuceneIndexSearcher class: {}", e)));
            return;
        }
    };

    let io_runtime = manager.io_runtime.clone();
    let cpu_executor = manager.cpu_executor();
    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };

    let is_explain: bool = is_query_plan_explain_enabled != 0;

    // Use spawn + blocking channel instead of block_on.
    // block_on occupies the calling thread as a worker, causing RepartitionExec's
    // spawned tasks to serialize on that thread. spawn() lets the io_runtime's
    // worker threads handle the tasks concurrently.
    let (tx, rx) = std::sync::mpsc::channel();

    io_runtime.spawn(async move {
        let result = indexed_query_executor::execute_indexed_query_stream(
            weight_ptr,
            seg_max_docs,
            pq_paths,
            tbl_name,
            plan_bytes_vec,
            n,
            mode,
            is_explain,
            jvm,
            searcher_class_ref,
            runtime,
            cpu_executor,
        ).await;
        let _ = tx.send(result);
    });

    let result = rx.recv().unwrap_or_else(|_| Err(DataFusionError::Execution("Channel closed".to_string())));

    match result {
        Ok(stream_ptr) => {
            set_action_listener_ok(&mut env, listener, stream_ptr);
        }
        Err(e) => {
            log_error!("Indexed query execution failed: {}", e);
            set_action_listener_error(&mut env, listener, &e);
        }
    }
}

/// Signals cancellation for the query registered under the given task ID.
/// The cancellation token is picked up by the select! in executeQueryPhaseAsync
/// and streamNext, causing them to return early to Java.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cancelQuery(
    _env: JNIEnv,
    _class: JClass,
    task_id: jlong,
) {
    if let Some(ctx) = ACTIVE_QUERIES.get(&task_id) {
        ctx.cancellation_token.cancel();
        // If the cpu task is already running, abort it immediately so the
        // cpu_executor thread is freed without waiting for Java to call streamClose
        // (which would achieve the same via JoinSet::abort_all on driver drop).
        if let Some(handle) = ctx.cpu_abort_handle.get() {
            handle.abort();
            log_info!("Cancelled query with task_id={} (cpu task aborted)", task_id);
        } else {
            log_info!("Cancelled query with task_id={} (cpu task not yet started)", task_id);
        }
    } else {
        log_debug!("cancelQuery called for unknown/completed task_id={}", task_id);
    }
}

/// Returns the number of currently registered in-flight queries.
/// Intended for testing only — not part of the production API.
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_getActiveQueryCount(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    ACTIVE_QUERIES.len() as jlong
}

#[cfg(test)]
mod active_queries_tests {
    use super::*;

    #[test]
    fn test_insert_and_remove() {
        let id: i64 = 99001;
        // Ensure clean state
        ACTIVE_QUERIES.remove(&id);

        ACTIVE_QUERIES.insert(id, QueryContext::new());
        assert!(ACTIVE_QUERIES.contains_key(&id), "entry should be present after insert");

        ACTIVE_QUERIES.remove(&id);
        assert!(!ACTIVE_QUERIES.contains_key(&id), "entry should be absent after remove");
    }

    #[test]
    fn test_remove_nonexistent_is_noop() {
        let id: i64 = 99002;
        ACTIVE_QUERIES.remove(&id);
        // Removing again should not panic
        ACTIVE_QUERIES.remove(&id);
    }

    #[test]
    fn test_cancellation_token_is_not_cancelled_initially() {
        let id: i64 = 99003;
        ACTIVE_QUERIES.remove(&id);

        ACTIVE_QUERIES.insert(id, QueryContext::new());
        let is_cancelled = ACTIVE_QUERIES
            .get(&id)
            .map(|ctx| ctx.cancellation_token.is_cancelled())
            .unwrap_or(false);
        assert!(!is_cancelled, "token should not be cancelled on creation");

        ACTIVE_QUERIES.remove(&id);
    }

    #[test]
    fn test_multiple_queries_have_independent_tokens() {
        let id_a: i64 = 99004;
        let id_b: i64 = 99005;
        ACTIVE_QUERIES.remove(&id_a);
        ACTIVE_QUERIES.remove(&id_b);

        ACTIVE_QUERIES.insert(id_a, QueryContext::new());
        ACTIVE_QUERIES.insert(id_b, QueryContext::new());

        // Cancel only query A
        if let Some(ctx) = ACTIVE_QUERIES.get(&id_a) {
            ctx.cancellation_token.cancel();
        }

        let a_cancelled = ACTIVE_QUERIES.get(&id_a)
            .map(|ctx| ctx.cancellation_token.is_cancelled())
            .unwrap_or(false);
        let b_cancelled = ACTIVE_QUERIES.get(&id_b)
            .map(|ctx| ctx.cancellation_token.is_cancelled())
            .unwrap_or(false);

        assert!(a_cancelled, "query A token should be cancelled");
        assert!(!b_cancelled, "query B token should remain uncancelled");

        ACTIVE_QUERIES.remove(&id_a);
        ACTIVE_QUERIES.remove(&id_b);
    }
}
