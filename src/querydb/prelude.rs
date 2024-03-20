use redb::DatabaseError;

pub enum StoreError {
    DatabaseInitilization(DatabaseError),
}
