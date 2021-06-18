use serde_closure::Fn;

fn main(){
    let one = 1;
    let plus_one = serde_closure::Fn!(|x: i32| x + one);
    println!("{:#?} {:#?}",plus_one, plus_one.call(1,));
}