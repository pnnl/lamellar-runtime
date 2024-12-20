use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

#[lamellar::AmData(Clone)]
struct StringDarcAm {
    new_data: String,
    data: LocalRwDarc<String>,
}

#[lamellar::am]
impl LamellarAm for StringDarcAm {
    async fn exec(self) {
        let mut data = self.data.write().await;
        data.clear();
        data.push_str(self.new_data.as_str());
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    world.clone().block_on(async move {
        let string_data = LocalRwDarc::new(&world, format!("Orig String on PE: {}", my_pe))
            .await
            .unwrap();

        println!("[PE: {}] {}", my_pe, string_data.read().await);

        if my_pe == 0 {
            world
                .exec_am_pe(
                    1,
                    StringDarcAm {
                        new_data: String::from("Modified string from 0"),
                        data: string_data.clone(),
                    },
                )
                .await;
        }
        world.async_barrier().await;
        println!("[PE: {}] {}", my_pe, string_data.read().await);
    });
}
