#![doc = "Native Parquet output and staged publication for Shopping Sessions V2."]
#![forbid(unsafe_code)]

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Array, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use shopping_sessions_domain::{
    CAMPAIGN_DAILY_SCHEMA, CampaignDailyRow, ColumnSchema, LogicalType, Phase, PipelineError,
    PipelineErrorKind, USER_DAILY_SCHEMA, UserDailyRow,
};

const OUTPUT_ROOT: &str = "/output";
const SCRATCH_ROOT: &str = "/scratch";
const PART_FILE: &str = "part-0.parquet";
pub const ROW_GROUP_ROWS: usize = 128 * 1024;
const ROW_GROUP_ROWS_I64: i64 = 128 * 1024;
pub const ZSTD_LEVEL: i32 = 1;

static STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

type Result<T> = std::result::Result<T, PipelineError>;

/// Publishes both output tables to the scorer-owned output root.
///
/// The scratch argument is deliberately fixed too: accepting arbitrary roots would let
/// orchestration accidentally write outside the container's two writable mounts.
///
/// # Errors
///
/// Returns an output error if staging, Parquet validation, cleanup, or publication fails.
pub fn publish(user_rows: &[UserDailyRow], campaign_rows: &[CampaignDailyRow]) -> Result<()> {
    publish_at(
        Path::new(OUTPUT_ROOT),
        Path::new(SCRATCH_ROOT),
        user_rows,
        campaign_rows,
    )
}

fn publish_at(
    output_root: &Path,
    scratch_root: &Path,
    user_rows: &[UserDailyRow],
    campaign_rows: &[CampaignDailyRow],
) -> Result<()> {
    // The scratch root is part of the boundary even though this writer's atomic stage must
    // reside on the output filesystem. Later spill integration can use only this validated root.
    if output_root != Path::new(OUTPUT_ROOT) || scratch_root != Path::new(SCRATCH_ROOT) {
        return Err(output_error(
            PipelineErrorKind::OutputValidation,
            "output publication is restricted to /output and /scratch",
        ));
    }
    publish_in_root(output_root, user_rows, campaign_rows)
}

fn publish_in_root(
    output_root: &Path,
    user_rows: &[UserDailyRow],
    campaign_rows: &[CampaignDailyRow],
) -> Result<()> {
    fs::create_dir_all(output_root).map_err(|error| io_error("create output root", error))?;

    let sequence = STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let stage = output_root.join(format!(
        ".v2-output-stage-{}-{sequence}",
        std::process::id()
    ));
    if stage.exists() {
        fs::remove_dir_all(&stage).map_err(|error| io_error("remove stale stage", error))?;
    }
    fs::create_dir(&stage).map_err(|error| io_error("create output stage", error))?;
    let mut cleanup = StageCleanup::new(stage.clone());

    let user_stage_dir = stage.join("user_daily");
    let campaign_stage_dir = stage.join("campaign_daily");
    fs::create_dir(&user_stage_dir).map_err(|error| io_error("create user stage", error))?;
    fs::create_dir(&campaign_stage_dir)
        .map_err(|error| io_error("create campaign stage", error))?;
    let user_stage = user_stage_dir.join(PART_FILE);
    let campaign_stage = campaign_stage_dir.join(PART_FILE);
    let user_batch = user_batch(user_rows)?;
    let campaign_batch = campaign_batch(campaign_rows)?;

    write_parquet(&user_stage, &user_batch)?;
    write_parquet(&campaign_stage, &campaign_batch)?;
    verify_parquet(&user_stage, &user_schema(), user_rows.len(), Some(5))?;
    verify_parquet(
        &campaign_stage,
        &campaign_schema(),
        campaign_rows.len(),
        None,
    )?;

    publish_table(output_root, "user_daily", &user_stage_dir)?;
    if let Err(error) = publish_table(output_root, "campaign_daily", &campaign_stage_dir) {
        // Never leave a successful-looking half-publication after a late failure.
        let _ = fs::remove_dir_all(output_root.join("user_daily"));
        return Err(error);
    }

    fs::remove_dir(&stage).map_err(|error| io_error("remove empty stage", error))?;
    cleanup.disarm();
    Ok(())
}

fn publish_table(output_root: &Path, table: &str, staged_dir: &Path) -> Result<()> {
    let table_dir = output_root.join(table);
    if table_dir.exists() {
        fs::remove_dir_all(&table_dir)
            .map_err(|error| io_error("remove previous output table", error))?;
    }
    fs::rename(staged_dir, &table_dir)
        .map_err(|error| io_error("atomically publish output table", error))
}

fn user_schema() -> Arc<Schema> {
    schema_for(&USER_DAILY_SCHEMA)
}

fn campaign_schema() -> Arc<Schema> {
    schema_for(&CAMPAIGN_DAILY_SCHEMA)
}

fn schema_for(contract: &[ColumnSchema]) -> Arc<Schema> {
    Arc::new(Schema::new(
        contract
            .iter()
            .map(|column| {
                let data_type = match column.logical_type {
                    LogicalType::Utf8 => DataType::Utf8,
                    LogicalType::Date => DataType::Date32,
                    LogicalType::SignedInteger => DataType::Int64,
                };
                Field::new(column.name, data_type, column.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}

fn user_batch(rows: &[UserDailyRow]) -> Result<RecordBatch> {
    let user_ids = rows
        .iter()
        .map(|row| row.user_id.as_str())
        .collect::<Vec<_>>();
    let days = rows.iter().map(|row| row.day.0).collect::<Vec<_>>();
    let sessions = rows.iter().map(|row| row.sessions).collect::<Vec<_>>();
    let events = rows.iter().map(|row| row.events).collect::<Vec<_>>();
    let revenue = rows.iter().map(|row| row.revenue_cents).collect::<Vec<_>>();
    let top_categories = rows
        .iter()
        .map(|row| {
            row.top_category
                .as_ref()
                .map(shopping_sessions_domain::OwnedUtf8::as_str)
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        user_schema(),
        vec![
            Arc::new(StringArray::from(user_ids)),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(sessions)),
            Arc::new(Int64Array::from(events)),
            Arc::new(Int64Array::from(revenue)),
            Arc::new(StringArray::from(top_categories)),
        ],
    )
    .map_err(|error| validation_error("construct user_daily batch", error))
}

fn campaign_batch(rows: &[CampaignDailyRow]) -> Result<RecordBatch> {
    let campaign_ids = rows
        .iter()
        .map(|row| row.campaign_id.as_str())
        .collect::<Vec<_>>();
    let channels = rows
        .iter()
        .map(|row| row.channel.as_str())
        .collect::<Vec<_>>();
    let days = rows.iter().map(|row| row.day.0).collect::<Vec<_>>();
    let sessions = rows.iter().map(|row| row.sessions).collect::<Vec<_>>();
    let purchasing = rows
        .iter()
        .map(|row| row.purchasing_sessions)
        .collect::<Vec<_>>();
    let revenue = rows.iter().map(|row| row.revenue_cents).collect::<Vec<_>>();
    let assisted = rows
        .iter()
        .map(|row| row.assisted_sessions)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        campaign_schema(),
        vec![
            Arc::new(StringArray::from(campaign_ids)),
            Arc::new(StringArray::from(channels)),
            Arc::new(Date32Array::from(days)),
            Arc::new(Int64Array::from(sessions)),
            Arc::new(Int64Array::from(purchasing)),
            Arc::new(Int64Array::from(revenue)),
            Arc::new(Int64Array::from(assisted)),
        ],
    )
    .map_err(|error| validation_error("construct campaign_daily batch", error))
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(ZSTD_LEVEL)
                .map_err(|error| validation_error("configure Zstd", error))?,
        ))
        .set_max_row_group_size(ROW_GROUP_ROWS)
        .build();
    let file = File::create(path).map_err(|error| io_error("create staged parquet", error))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .map_err(|error| validation_error("create Parquet writer", error))?;
    writer
        .write(batch)
        .map_err(|error| validation_error("write Parquet batch", error))?;
    writer
        .close()
        .map_err(|error| validation_error("close Parquet writer", error))?;
    Ok(())
}

fn verify_parquet(
    path: &Path,
    expected_schema: &Arc<Schema>,
    expected_rows: usize,
    nullable_value_column: Option<usize>,
) -> Result<()> {
    let file = File::open(path).map_err(|error| io_error("open staged parquet", error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| validation_error("read staged Parquet metadata", error))?;

    if builder.schema().as_ref() != expected_schema.as_ref() {
        return Err(output_error(
            PipelineErrorKind::OutputValidation,
            "staged Parquet schema does not match the frozen output schema",
        ));
    }
    let metadata = builder.metadata();
    for row_group in metadata.row_groups() {
        if row_group.num_rows() > ROW_GROUP_ROWS_I64 {
            return Err(output_error(
                PipelineErrorKind::OutputValidation,
                "staged Parquet row group exceeds 128K rows",
            ));
        }
        for column in row_group.columns() {
            if !matches!(column.compression(), Compression::ZSTD(level) if level.compression_level() == ZSTD_LEVEL)
            {
                return Err(output_error(
                    PipelineErrorKind::OutputValidation,
                    "staged Parquet column is not compressed with Zstd level 1",
                ));
            }
        }
    }

    let mut rows = 0_usize;
    let reader = builder
        .build()
        .map_err(|error| validation_error("create staged Parquet reader", error))?;
    for batch in reader {
        let batch = batch.map_err(|error| validation_error("read staged Parquet rows", error))?;
        rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
            output_error(
                PipelineErrorKind::OutputValidation,
                "staged Parquet row count overflow",
            )
        })?;
        for (index, column) in batch.columns().iter().enumerate() {
            if Some(index) != nullable_value_column && column.null_count() != 0 {
                return Err(output_error(
                    PipelineErrorKind::OutputValidation,
                    "only user_daily.top_category values may be null",
                ));
            }
        }
    }
    if rows != expected_rows {
        return Err(output_error(
            PipelineErrorKind::OutputValidation,
            "staged Parquet readback row count differs from input",
        ));
    }
    Ok(())
}

fn io_error(context: &str, error: impl std::fmt::Display) -> PipelineError {
    output_error(PipelineErrorKind::Io, format!("{context}: {error}"))
}

fn validation_error(context: &str, error: impl std::fmt::Display) -> PipelineError {
    output_error(
        PipelineErrorKind::OutputValidation,
        format!("{context}: {error}"),
    )
}

fn output_error(kind: PipelineErrorKind, message: impl Into<Box<str>>) -> PipelineError {
    PipelineError::new(kind, message).in_phase(Phase::Write)
}

struct StageCleanup {
    path: PathBuf,
    armed: bool,
}

impl StageCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StageCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, Int64Array, StringArray};
    use shopping_sessions_domain::{EpochDay, OwnedUtf8};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn temporary_output() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "v2-output-test-{}-{nonce}-{sequence}",
            std::process::id()
        ))
    }

    fn read_one(path: &Path) -> RecordBatch {
        let file = File::open(path).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        reader.next().unwrap().unwrap()
    }

    fn read_schema_and_row_count(path: &Path) -> (Arc<Schema>, usize) {
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let schema = Arc::clone(builder.schema());
        let rows = builder
            .build()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum();
        (schema, rows)
    }

    fn assert_layout(root: &Path) {
        let mut root_entries = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        root_entries.sort();
        assert_eq!(root_entries, ["campaign_daily", "user_daily"]);
        for table in ["user_daily", "campaign_daily"] {
            let entries = fs::read_dir(root.join(table))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            assert_eq!(entries, [PART_FILE]);
        }
    }

    #[test]
    fn publishes_nonempty_exact_values_and_layout() {
        let root = temporary_output();
        let users = vec![UserDailyRow {
            user_id: OwnedUtf8::from_static("u-é"),
            day: EpochDay(20_000),
            sessions: 2,
            events: 7,
            revenue_cents: -5,
            top_category: None,
        }];
        let campaigns = vec![CampaignDailyRow {
            campaign_id: OwnedUtf8::from_static("c1"),
            channel: OwnedUtf8::from_static("paid"),
            day: EpochDay(20_000),
            sessions: 1,
            purchasing_sessions: 1,
            revenue_cents: 99,
            assisted_sessions: 3,
        }];

        publish_in_root(&root, &users, &campaigns).unwrap();
        assert_layout(&root);

        let user = read_one(&root.join("user_daily").join(PART_FILE));
        assert_eq!(user.schema(), user_schema());
        assert_eq!(
            user.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "u-é"
        );
        assert_eq!(
            user.column(1)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            20_000
        );
        assert_eq!(
            user.column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            -5
        );
        assert!(user.column(5).is_null(0));

        let campaign = read_one(&root.join("campaign_daily").join(PART_FILE));
        assert_eq!(campaign.schema(), campaign_schema());
        assert_eq!(
            campaign
                .column(6)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publishes_typed_empty_tables_and_replaces_stale_output() {
        let root = temporary_output();
        fs::create_dir_all(root.join("user_daily")).unwrap();
        fs::write(root.join("user_daily").join("stale.txt"), b"stale").unwrap();
        fs::create_dir_all(root.join("campaign_daily")).unwrap();
        fs::write(root.join("campaign_daily").join("old.parquet"), b"old").unwrap();

        publish_in_root(&root, &[], &[]).unwrap();
        assert_layout(&root);
        let (user_schema_read, user_rows) =
            read_schema_and_row_count(&root.join("user_daily").join(PART_FILE));
        let (campaign_schema_read, campaign_rows) =
            read_schema_and_row_count(&root.join("campaign_daily").join(PART_FILE));
        assert_eq!(user_rows, 0);
        assert_eq!(campaign_rows, 0);
        assert_eq!(user_schema_read, user_schema());
        assert_eq!(campaign_schema_read, campaign_schema());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn writer_uses_128k_zstd1_row_groups_and_contract_optionality() {
        let root = temporary_output();
        fs::create_dir_all(&root).unwrap();
        let count = ROW_GROUP_ROWS + 1;
        let batch = RecordBatch::try_new(
            user_schema(),
            vec![
                Arc::new(StringArray::from(vec!["u"; count])),
                Arc::new(Date32Array::from(vec![0; count])),
                Arc::new(Int64Array::from(vec![1; count])),
                Arc::new(Int64Array::from(vec![1; count])),
                Arc::new(Int64Array::from(vec![0; count])),
                Arc::new(StringArray::from(vec![Some("cat"); count])),
            ],
        )
        .unwrap();
        let path = root.join(PART_FILE);
        write_parquet(&path, &batch).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
        let metadata = builder.metadata();
        assert_eq!(
            metadata
                .row_groups()
                .iter()
                .map(parquet::file::metadata::RowGroupMetaData::num_rows)
                .collect::<Vec<_>>(),
            [ROW_GROUP_ROWS_I64, 1]
        );
        assert!(metadata.row_groups().iter().all(|group| group
            .columns()
            .iter()
            .all(|column| matches!(column.compression(), Compression::ZSTD(level) if level.compression_level() == ZSTD_LEVEL))));
        assert_eq!(
            metadata
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .map(|column| column.max_def_level())
                .collect::<Vec<_>>(),
            [0, 0, 0, 0, 0, 1]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn public_boundary_rejects_arbitrary_write_roots() {
        let root = temporary_output();
        let error = publish_at(&root, Path::new(SCRATCH_ROOT), &[], &[]).unwrap_err();
        assert_eq!(error.kind, PipelineErrorKind::OutputValidation);
        assert!(!root.exists());
    }

    #[test]
    fn failed_publication_removes_stage_and_partial_table() {
        let root = temporary_output();
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("campaign_daily"), b"blocks table directory").unwrap();

        let error = publish_in_root(&root, &[], &[]).unwrap_err();
        assert_eq!(error.kind, PipelineErrorKind::Io);
        assert!(!root.join("user_daily").exists());
        assert!(root.join("campaign_daily").is_file());
        assert!(fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".v2-output-stage-")
        }));
        fs::remove_dir_all(root).unwrap();
    }
}
