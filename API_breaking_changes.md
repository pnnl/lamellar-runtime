### API breaking changes ###
from V 0.4.x -> V 0.5.x

## Active Message Interfaces: ##
split exec_on_pe and exec_am_all in two seperate request types (traits)

# single pe #
individual pe requests return a T directly (removed wrapping in an Option)
and removed the get_all function call

```Rust
pub trait LamellarRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn get(&self) -> Self::Output;
}
```
example of change:
```Rust
let world: LamellarWorld =...;

//old API
let req: Box<dyn LamellarRequest> = world.exec_am_pe(pe,am);
let val: T = req.get().unwrap();
//new API
let req: Box<dyn LamellarRequest> = world.exec_am_pe(pe,am);
let val: T = req.get();
```

# multi pe #
multi pe requests return a vector of T's directly (not wrapped in an Option)
since this is now its own trait we also just use "get" as the function name rather than "get_all"

```Rust
pub trait LamellarMultiRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Vec<Self::Output>;
    fn get(&self) -> Vec<Self::Output>;
}
```
example of change:
```Rust
let world: LamellarWorld =...;

//old API -notice it also returned a LamellarRequest trait object
let req: Box<dyn LamellarRequest> = world.exec_am_all(am);
let val: Vec<T> = req.get_all().unwrap();
//new API - now returns a LamellarMultiRequest trait object
let req: Box<dyn LamellarMultiRequest> = world.exec_am_all(am);
let val Vec<T> = req.get();
```

