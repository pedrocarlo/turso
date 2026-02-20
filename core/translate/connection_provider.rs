use crate::connection::AttachedDatabasesFingerprint;
use crate::io::FileSyncType;
use crate::schema::{Schema, Trigger};
use crate::storage::encryption::{CipherMode, EncryptionKey};
use crate::storage::page_cache::CacheResizeResult;
use crate::storage::pager::AutoVacuumMode;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::sqlite3_ondisk::PageSize;
use crate::sync::Arc;
use crate::util::IOExt as _;
use crate::{
    CaptureDataChangesInfo, Connection, DatabaseCatalog, Result, RwLock, SyncMode, TempStore,
};
use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;
use std::time::Duration;

pub(crate) trait ConnectionProvider {
    fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>>;
    fn attached_databases(&self) -> &RwLock<DatabaseCatalog>;
    fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>>;

    // Getters
    fn get_busy_timeout(&self) -> Duration;
    fn get_cache_size(&self) -> i32;
    fn get_page_size(&self) -> PageSize;
    fn get_query_only(&self) -> bool;
    fn get_encryption_cipher_mode(&self) -> Option<CipherMode>;
    fn get_sync_mode(&self) -> SyncMode;
    fn get_data_sync_retry(&self) -> bool;
    fn get_temp_store(&self) -> TempStore;
    fn get_sync_type(&self) -> FileSyncType;
    fn get_syms_vtab_mods(&self) -> HashSet<String>;
    fn get_syms_functions(&self) -> Vec<(String, bool, i32)>;
    fn list_all_databases(&self) -> Vec<(usize, String, String)>;
    fn mvcc_enabled(&self) -> bool;
    fn mvcc_checkpoint_threshold(&self) -> Result<i64>;
    fn foreign_keys_enabled(&self) -> bool;
    fn check_constraints_ignored(&self) -> bool;
    fn enable_autovacuum(&self) -> bool;
    fn encryption_key_is_set(&self) -> bool;
    fn encryption_cipher(&self) -> CipherMode;
    fn database_ptr(&self) -> usize;
    fn syms_generation(&self) -> u64;
    fn attached_databases_fingerprint(&self) -> AttachedDatabasesFingerprint;
    fn get_spill_enabled(&self) -> bool;

    // Setters
    fn set_busy_timeout(&self, duration: Duration);
    fn set_cache_size(&self, size: i32);
    fn set_query_only(&self, value: bool);
    fn set_encryption_key(&self, key: EncryptionKey) -> Result<()>;
    fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()>;
    fn set_sync_mode(&self, mode: SyncMode);
    fn set_data_sync_retry(&self, value: bool);
    fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()>;
    fn set_foreign_keys_enabled(&self, enable: bool);
    fn set_check_constraints_ignored(&self, ignore: bool);
    fn set_sync_type(&self, value: FileSyncType);
    fn set_temp_store(&self, value: TempStore);
    fn reset_page_size(&self, size: u32) -> Result<()>;
    fn experimental_attach_enabled(&self) -> bool;
    fn experimental_index_method_enabled(&self) -> bool;
    fn experimental_triggers_enabled(&self) -> bool;
    fn experimental_views_enabled(&self) -> bool;
    fn is_nested_stmt(&self) -> bool;
    fn is_mvcc_bootstrap_connection(&self) -> bool;
    fn experimental_strict_enabled(&self) -> bool;
    fn trigger_is_compiling(&self, trigger: impl AsRef<Trigger>) -> bool;
    fn start_trigger_compilation(&self, trigger: Arc<Trigger>);
    fn end_trigger_compilation(&self);
    /// Set whether cache spilling is enabled.
    fn set_spill_enabled(&self, enabled: bool);

    // Pager-wrapping methods
    fn get_auto_vacuum_mode(&self) -> AutoVacuumMode;
    fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode);
    fn freepage_list(&self) -> u32;
    fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<T>;
    fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<T>;
    fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult>;
}

impl ConnectionProvider for Connection {
    fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        self.database_schemas()
    }

    fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        self.attached_databases()
    }

    fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        self.get_capture_data_changes_info()
    }

    fn get_busy_timeout(&self) -> Duration {
        self.get_busy_timeout()
    }

    fn get_cache_size(&self) -> i32 {
        self.get_cache_size()
    }

    fn get_page_size(&self) -> PageSize {
        self.get_page_size()
    }

    fn get_query_only(&self) -> bool {
        self.get_query_only()
    }

    fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        self.get_encryption_cipher_mode()
    }

    fn get_sync_mode(&self) -> SyncMode {
        self.get_sync_mode()
    }

    fn get_data_sync_retry(&self) -> bool {
        self.get_data_sync_retry()
    }

    fn get_temp_store(&self) -> TempStore {
        self.get_temp_store()
    }

    fn get_sync_type(&self) -> FileSyncType {
        self.get_sync_type()
    }

    fn get_syms_vtab_mods(&self) -> HashSet<String> {
        self.get_syms_vtab_mods()
    }

    fn get_syms_functions(&self) -> Vec<(String, bool, i32)> {
        self.get_syms_functions()
    }

    fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        self.list_all_databases()
    }

    fn mvcc_enabled(&self) -> bool {
        self.mvcc_enabled()
    }

    fn mvcc_checkpoint_threshold(&self) -> Result<i64> {
        self.mvcc_checkpoint_threshold()
    }

    fn foreign_keys_enabled(&self) -> bool {
        self.foreign_keys_enabled()
    }

    fn check_constraints_ignored(&self) -> bool {
        self.check_constraints_ignored()
    }

    fn enable_autovacuum(&self) -> bool {
        self.db.opts.enable_autovacuum
    }

    fn encryption_key_is_set(&self) -> bool {
        self.encryption_key.read().is_some()
    }

    fn encryption_cipher(&self) -> CipherMode {
        self.encryption_cipher_mode.get()
    }

    fn database_ptr(&self) -> usize {
        self.database_ptr()
    }

    fn syms_generation(&self) -> u64 {
        self.syms_generation()
    }

    fn attached_databases_fingerprint(&self) -> AttachedDatabasesFingerprint {
        self.attached_databases_fingerprint()
    }

    fn set_busy_timeout(&self, duration: Duration) {
        self.set_busy_timeout(duration);
    }

    fn set_cache_size(&self, size: i32) {
        self.set_cache_size(size);
    }

    fn set_query_only(&self, value: bool) {
        self.set_query_only(value);
    }

    fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        self.set_encryption_key(key)
    }

    fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        self.set_encryption_cipher(cipher_mode)
    }

    fn set_sync_mode(&self, mode: SyncMode) {
        self.set_sync_mode(mode);
    }

    fn set_data_sync_retry(&self, value: bool) {
        self.set_data_sync_retry(value);
    }

    fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        self.set_mvcc_checkpoint_threshold(threshold)
    }

    fn set_foreign_keys_enabled(&self, enable: bool) {
        self.set_foreign_keys_enabled(enable);
    }

    fn set_check_constraints_ignored(&self, ignore: bool) {
        self.set_check_constraints_ignored(ignore);
    }

    fn set_sync_type(&self, value: FileSyncType) {
        self.set_sync_type(value);
    }

    fn set_temp_store(&self, value: TempStore) {
        self.set_temp_store(value);
    }

    fn reset_page_size(&self, size: u32) -> Result<()> {
        self.reset_page_size(size)
    }

    fn experimental_attach_enabled(&self) -> bool {
        self.experimental_attach_enabled()
    }

    fn experimental_index_method_enabled(&self) -> bool {
        self.experimental_index_method_enabled()
    }

    fn experimental_triggers_enabled(&self) -> bool {
        self.experimental_triggers_enabled()
    }

    fn experimental_views_enabled(&self) -> bool {
        self.experimental_views_enabled()
    }

    fn is_nested_stmt(&self) -> bool {
        self.is_nested_stmt()
    }

    fn is_mvcc_bootstrap_connection(&self) -> bool {
        self.is_mvcc_bootstrap_connection()
    }

    fn experimental_strict_enabled(&self) -> bool {
        self.experimental_strict_enabled()
    }

    fn trigger_is_compiling(&self, trigger: impl AsRef<Trigger>) -> bool {
        Connection::trigger_is_compiling(self, trigger)
    }

    fn start_trigger_compilation(&self, trigger: Arc<Trigger>) {
        Connection::start_trigger_compilation(self, trigger)
    }

    fn end_trigger_compilation(&self) {
        Connection::end_trigger_compilation(self)
    }

    fn get_spill_enabled(&self) -> bool {
        self.get_pager().get_spill_enabled()
    }

    fn set_spill_enabled(&self, enabled: bool) {
        self.get_pager().set_spill_enabled(enabled);
    }

    fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        self.get_pager().get_auto_vacuum_mode()
    }

    fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        self.get_pager().set_auto_vacuum_mode(mode)
    }

    fn freepage_list(&self) -> u32 {
        self.get_pager().freepage_list()
    }

    fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<T> {
        let pager = self.get_pager();
        pager.io.block(|| pager.with_header(&f))
    }

    fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<T> {
        let pager = self.get_pager();
        pager.io.block(|| pager.with_header_mut(&f))
    }

    fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        self.get_pager().change_page_cache_size(capacity)
    }
}

impl ConnectionProvider for Arc<Connection> {
    fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        self.as_ref().database_schemas()
    }

    fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        self.as_ref().attached_databases()
    }

    fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        self.as_ref().get_capture_data_changes_info()
    }

    fn get_busy_timeout(&self) -> Duration {
        self.as_ref().get_busy_timeout()
    }

    fn get_cache_size(&self) -> i32 {
        self.as_ref().get_cache_size()
    }

    fn get_page_size(&self) -> PageSize {
        self.as_ref().get_page_size()
    }

    fn get_query_only(&self) -> bool {
        self.as_ref().get_query_only()
    }

    fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        self.as_ref().get_encryption_cipher_mode()
    }

    fn get_sync_mode(&self) -> SyncMode {
        self.as_ref().get_sync_mode()
    }

    fn get_data_sync_retry(&self) -> bool {
        self.as_ref().get_data_sync_retry()
    }

    fn get_temp_store(&self) -> TempStore {
        self.as_ref().get_temp_store()
    }

    fn get_sync_type(&self) -> FileSyncType {
        self.as_ref().get_sync_type()
    }

    fn get_syms_vtab_mods(&self) -> HashSet<String> {
        self.as_ref().get_syms_vtab_mods()
    }

    fn get_syms_functions(&self) -> Vec<(String, bool, i32)> {
        self.as_ref().get_syms_functions()
    }

    fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        self.as_ref().list_all_databases()
    }

    fn mvcc_enabled(&self) -> bool {
        self.as_ref().mvcc_enabled()
    }

    fn mvcc_checkpoint_threshold(&self) -> Result<i64> {
        self.as_ref().mvcc_checkpoint_threshold()
    }

    fn foreign_keys_enabled(&self) -> bool {
        self.as_ref().foreign_keys_enabled()
    }

    fn check_constraints_ignored(&self) -> bool {
        self.as_ref().check_constraints_ignored()
    }

    fn enable_autovacuum(&self) -> bool {
        self.as_ref().enable_autovacuum()
    }

    fn encryption_key_is_set(&self) -> bool {
        self.as_ref().encryption_key_is_set()
    }

    fn encryption_cipher(&self) -> CipherMode {
        self.as_ref().encryption_cipher()
    }

    fn database_ptr(&self) -> usize {
        self.as_ref().database_ptr()
    }

    fn syms_generation(&self) -> u64 {
        self.as_ref().syms_generation()
    }

    fn attached_databases_fingerprint(&self) -> AttachedDatabasesFingerprint {
        self.as_ref().attached_databases_fingerprint()
    }

    fn set_busy_timeout(&self, duration: Duration) {
        self.as_ref().set_busy_timeout(duration);
    }

    fn set_cache_size(&self, size: i32) {
        self.as_ref().set_cache_size(size);
    }

    fn set_query_only(&self, value: bool) {
        self.as_ref().set_query_only(value);
    }

    fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        self.as_ref().set_encryption_key(key)
    }

    fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        self.as_ref().set_encryption_cipher(cipher_mode)
    }

    fn set_sync_mode(&self, mode: SyncMode) {
        self.as_ref().set_sync_mode(mode);
    }

    fn set_data_sync_retry(&self, value: bool) {
        self.as_ref().set_data_sync_retry(value);
    }

    fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        self.as_ref().set_mvcc_checkpoint_threshold(threshold)
    }

    fn set_foreign_keys_enabled(&self, enable: bool) {
        self.as_ref().set_foreign_keys_enabled(enable);
    }

    fn set_check_constraints_ignored(&self, ignore: bool) {
        self.as_ref().set_check_constraints_ignored(ignore);
    }

    fn set_sync_type(&self, value: FileSyncType) {
        self.as_ref().set_sync_type(value);
    }

    fn set_temp_store(&self, value: TempStore) {
        self.as_ref().set_temp_store(value);
    }

    fn reset_page_size(&self, size: u32) -> Result<()> {
        self.as_ref().reset_page_size(size)
    }

    fn experimental_attach_enabled(&self) -> bool {
        self.as_ref().experimental_attach_enabled()
    }

    fn experimental_index_method_enabled(&self) -> bool {
        self.as_ref().experimental_index_method_enabled()
    }

    fn experimental_triggers_enabled(&self) -> bool {
        self.as_ref().experimental_triggers_enabled()
    }

    fn experimental_views_enabled(&self) -> bool {
        self.as_ref().experimental_views_enabled()
    }

    fn is_nested_stmt(&self) -> bool {
        self.as_ref().is_nested_stmt()
    }

    fn is_mvcc_bootstrap_connection(&self) -> bool {
        self.as_ref().is_mvcc_bootstrap_connection()
    }

    fn experimental_strict_enabled(&self) -> bool {
        self.as_ref().experimental_strict_enabled()
    }

    fn trigger_is_compiling(&self, trigger: impl AsRef<Trigger>) -> bool {
        self.as_ref().trigger_is_compiling(trigger)
    }

    fn start_trigger_compilation(&self, trigger: Arc<Trigger>) {
        self.as_ref().start_trigger_compilation(trigger)
    }

    fn end_trigger_compilation(&self) {
        self.as_ref().end_trigger_compilation()
    }

    fn get_spill_enabled(&self) -> bool {
        self.as_ref().get_spill_enabled()
    }

    fn set_spill_enabled(&self, enabled: bool) {
        self.as_ref().set_spill_enabled(enabled);
    }

    fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        self.as_ref().get_auto_vacuum_mode()
    }

    fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        self.as_ref().set_auto_vacuum_mode(mode)
    }

    fn freepage_list(&self) -> u32 {
        self.as_ref().freepage_list()
    }

    fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<T> {
        self.as_ref().with_header(f)
    }

    fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<T> {
        self.as_ref().with_header_mut(f)
    }

    fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        self.as_ref().change_page_cache_size(capacity)
    }
}

impl<C: ConnectionProvider> ConnectionProvider for &C {
    fn database_schemas(&self) -> &RwLock<HashMap<usize, Arc<Schema>>> {
        (*self).database_schemas()
    }

    fn attached_databases(&self) -> &RwLock<DatabaseCatalog> {
        (*self).attached_databases()
    }

    fn get_capture_data_changes_info(
        &self,
    ) -> crate::sync::RwLockReadGuard<'_, Option<CaptureDataChangesInfo>> {
        (*self).get_capture_data_changes_info()
    }

    fn get_busy_timeout(&self) -> Duration {
        (*self).get_busy_timeout()
    }

    fn get_cache_size(&self) -> i32 {
        (*self).get_cache_size()
    }

    fn get_page_size(&self) -> PageSize {
        (*self).get_page_size()
    }

    fn get_query_only(&self) -> bool {
        (*self).get_query_only()
    }

    fn get_encryption_cipher_mode(&self) -> Option<CipherMode> {
        (*self).get_encryption_cipher_mode()
    }

    fn get_sync_mode(&self) -> SyncMode {
        (*self).get_sync_mode()
    }

    fn get_data_sync_retry(&self) -> bool {
        (*self).get_data_sync_retry()
    }

    fn get_temp_store(&self) -> TempStore {
        (*self).get_temp_store()
    }

    fn get_sync_type(&self) -> FileSyncType {
        (*self).get_sync_type()
    }

    fn get_syms_vtab_mods(&self) -> HashSet<String> {
        (*self).get_syms_vtab_mods()
    }

    fn get_syms_functions(&self) -> Vec<(String, bool, i32)> {
        (*self).get_syms_functions()
    }

    fn list_all_databases(&self) -> Vec<(usize, String, String)> {
        (*self).list_all_databases()
    }

    fn mvcc_enabled(&self) -> bool {
        (*self).mvcc_enabled()
    }

    fn mvcc_checkpoint_threshold(&self) -> Result<i64> {
        (*self).mvcc_checkpoint_threshold()
    }

    fn foreign_keys_enabled(&self) -> bool {
        (*self).foreign_keys_enabled()
    }

    fn check_constraints_ignored(&self) -> bool {
        (*self).check_constraints_ignored()
    }

    fn enable_autovacuum(&self) -> bool {
        (*self).enable_autovacuum()
    }

    fn encryption_key_is_set(&self) -> bool {
        (*self).encryption_key_is_set()
    }

    fn encryption_cipher(&self) -> CipherMode {
        (*self).encryption_cipher()
    }

    fn database_ptr(&self) -> usize {
        (*self).database_ptr()
    }

    fn syms_generation(&self) -> u64 {
        (*self).syms_generation()
    }

    fn attached_databases_fingerprint(&self) -> AttachedDatabasesFingerprint {
        (*self).attached_databases_fingerprint()
    }

    fn set_busy_timeout(&self, duration: Duration) {
        (*self).set_busy_timeout(duration);
    }

    fn set_cache_size(&self, size: i32) {
        (*self).set_cache_size(size);
    }

    fn set_query_only(&self, value: bool) {
        (*self).set_query_only(value);
    }

    fn set_encryption_key(&self, key: EncryptionKey) -> Result<()> {
        (*self).set_encryption_key(key)
    }

    fn set_encryption_cipher(&self, cipher_mode: CipherMode) -> Result<()> {
        (*self).set_encryption_cipher(cipher_mode)
    }

    fn set_sync_mode(&self, mode: SyncMode) {
        (*self).set_sync_mode(mode);
    }

    fn set_data_sync_retry(&self, value: bool) {
        (*self).set_data_sync_retry(value);
    }

    fn set_mvcc_checkpoint_threshold(&self, threshold: i64) -> Result<()> {
        (*self).set_mvcc_checkpoint_threshold(threshold)
    }

    fn set_foreign_keys_enabled(&self, enable: bool) {
        (*self).set_foreign_keys_enabled(enable);
    }

    fn set_check_constraints_ignored(&self, ignore: bool) {
        (*self).set_check_constraints_ignored(ignore);
    }

    fn set_sync_type(&self, value: FileSyncType) {
        (*self).set_sync_type(value);
    }

    fn set_temp_store(&self, value: TempStore) {
        (*self).set_temp_store(value);
    }

    fn reset_page_size(&self, size: u32) -> Result<()> {
        (*self).reset_page_size(size)
    }

    fn experimental_attach_enabled(&self) -> bool {
        (*self).experimental_attach_enabled()
    }

    fn experimental_index_method_enabled(&self) -> bool {
        (*self).experimental_index_method_enabled()
    }

    fn experimental_triggers_enabled(&self) -> bool {
        (*self).experimental_triggers_enabled()
    }

    fn experimental_views_enabled(&self) -> bool {
        (*self).experimental_views_enabled()
    }

    fn is_nested_stmt(&self) -> bool {
        (*self).is_nested_stmt()
    }

    fn is_mvcc_bootstrap_connection(&self) -> bool {
        (*self).is_mvcc_bootstrap_connection()
    }

    fn experimental_strict_enabled(&self) -> bool {
        (*self).experimental_strict_enabled()
    }

    fn trigger_is_compiling(&self, trigger: impl AsRef<Trigger>) -> bool {
        (*self).trigger_is_compiling(trigger)
    }

    fn start_trigger_compilation(&self, trigger: Arc<Trigger>) {
        (*self).start_trigger_compilation(trigger)
    }

    fn end_trigger_compilation(&self) {
        (*self).end_trigger_compilation()
    }

    fn get_spill_enabled(&self) -> bool {
        (*self).get_spill_enabled()
    }

    fn set_spill_enabled(&self, enabled: bool) {
        (*self).set_spill_enabled(enabled)
    }

    fn get_auto_vacuum_mode(&self) -> AutoVacuumMode {
        (*self).get_auto_vacuum_mode()
    }

    fn set_auto_vacuum_mode(&self, mode: AutoVacuumMode) {
        (*self).set_auto_vacuum_mode(mode)
    }

    fn freepage_list(&self) -> u32 {
        (*self).freepage_list()
    }

    fn with_header<T>(&self, f: impl Fn(&DatabaseHeader) -> T) -> Result<T> {
        (*self).with_header(f)
    }

    fn with_header_mut<T>(&self, f: impl Fn(&mut DatabaseHeader) -> T) -> Result<T> {
        (*self).with_header_mut(f)
    }

    fn change_page_cache_size(&self, capacity: usize) -> Result<CacheResizeResult> {
        (*self).change_page_cache_size(capacity)
    }
}
