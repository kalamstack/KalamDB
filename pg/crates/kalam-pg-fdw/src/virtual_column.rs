/// Virtual columns synthesized by the FDW layer instead of the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualColumn {
    UserId,
    Seq,
    Deleted,
}
