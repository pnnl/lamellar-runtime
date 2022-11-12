pub mod distributed_iterator;
pub mod local_iterator;
pub mod one_sided_iterator;

/// The Schedule type controls how elements of a LamellarArray are distributed to threads when 
/// calling `for_each_with_schedule` on a local or distributed iterator.
/// 
/// Inspired by then OpenMP schedule parameter
///
/// # Possible Options
/// - Static: Each thread recieves a static range of elements to iterate over, the range length is roughly array.local_data().len()/number of threads on pe
/// - Dynaimc: Each thread processes a single element at a time
/// - Chunk(usize): Each thread prcesses chunk sized range of elements at a time.
/// - Guided: Similar to chunks, but the chunks decrease in size over time
/// - WorkStealing: Intially allocated the same range as static, but allows idle threads to steal work from busy threads
#[derive(Debug, Clone)]
pub enum Schedule {
    Static,
    Dynamic,      //single element
    Chunk(usize), //dynamic but with multiple elements
    Guided,       // chunks that get smaller over time
    WorkStealing, // static initially but other threads can steal
}
