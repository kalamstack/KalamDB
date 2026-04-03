pub mod audit;
pub mod guards;
pub mod storage;
pub mod table_creation;

#[macro_export]
macro_rules! register_typed_handler {
	($registry:expr, $kind:expr, $handler:expr, $variant:path $(,)?) => {
		$registry.register_typed($kind, $handler, |stmt| match stmt.kind() {
			$variant(statement) => Some(statement.clone()),
			_ => None,
		});
	};
}

#[macro_export]
macro_rules! register_dynamic_handler {
	($registry:expr, $kind:expr, $handler:expr $(,)?) => {
		$registry.register_dynamic($kind, $handler);
	};
}