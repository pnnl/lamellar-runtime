use std::any;

#[cfg(feature = "SocketsBackend")]
use crate::runtime::Arch;
#[cfg(feature = "SocketsBackend")]
use std::env;

#[allow(dead_code)]
pub fn print_type_of<T>(_: &T) {
    println!("{}", any::type_name::<T>());
}

// serialize the trait object F
// #[flame]
#[cfg(feature = "nightly")]
pub(crate) fn ser_closure<
    F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    T: any::Any + serde::ser::Serialize + serde::de::DeserializeOwned,
>(
    start: F,
) -> Vec<u8> {
    let arg: Vec<u8> = bincode::serialize(&start).unwrap();
    let start: serde_closure::FnOnce<(Vec<u8>,), fn((Vec<u8>,), ()) -> _> = FnOnce!([arg]move||{
        let arg: Vec<u8> = arg;
        let closure: F = bincode::deserialize(&arg).unwrap();
        closure()
    });
    bincode::serialize(&start).unwrap()
}

#[cfg(feature = "SocketsBackend")]
pub(crate) fn parse_slurm() -> Arch {
    //--------------parse slurm environment variables-----------------
    let num_locales = match env::var("SLURM_NNODES") {
        Ok(val) => val.parse::<usize>().unwrap(),
        Err(_e) => {
            println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
            1
        }
    };

    let num_pes = match env::var("PMI_SIZE") {
        Ok(val) => val.parse::<usize>().unwrap(),
        Err(_e) => {
            println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
            1
        }
    };

    let my_pe = match env::var("PMI_RANK") {
        Ok(val) => val.parse::<usize>().unwrap(),
        Err(_e) => {
            println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
            0
        }
    };

    let job_id = match env::var("SLURM_JOBID") {
        Ok(val) => val.parse::<usize>().unwrap(),
        Err(_e) => 1,
    };

    let my_name = match env::var("SLURMD_NODENAME") {
        Ok(val) => val,
        Err(_e) => {
            println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
            String::from("localhost")
        }
    };

    let nodes = match env::var("SLURM_NODELIST") {
        Ok(val) => val,
        Err(_e) => {
            println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
            String::from("localhost")
        }
    };

    let split = nodes.split(']').collect::<Vec<&str>>()[0]
        .split('[')
        .collect::<Vec<&str>>();

    let base = split[0];
    let mut pe_addrs: Vec<_> = Vec::new();
    let num_pe_node = num_pes / num_locales;

    if split.len() > 1 {
        for node_str in split[1].split(',') {
            let node_range = node_str.split('-').collect::<Vec<&str>>();
            if node_range.len() > 1 {
                for i in node_range[0].parse::<usize>().unwrap()
                    ..=node_range[1].parse::<usize>().unwrap()
                //..= is inclusive range
                {
                    let nn = format!("{:0width$}", i, width = node_range[0].len());
                    let tmp = [base, &nn[..]].concat();
                    if tmp == my_name {
                        // my_pe = pe_cnt;
                    }
                    for _i in 0..num_pe_node {
                        pe_addrs.push(tmp.clone());
                        // pe_cnt += 1;
                    }
                }
            } else {
                let tmp = [base, node_range[0]].concat();
                if tmp == my_name {
                    // my_pe = pe_cnt;
                }
                for _i in 0..num_pe_node {
                    pe_addrs.push(tmp.clone());
                    // pe_cnt += 1;
                }
            }
        }
    } else {
        for _i in 0..num_pe_node {
            pe_addrs.push("localhost".to_string());
        }
    }
    Arch {
        my_pe: my_pe,
        num_pes: num_pes,
        pe_addrs: pe_addrs,
        job_id: job_id,
    }
}

#[cfg(feature = "SocketsBackend")]
pub(crate) fn parse_localhost() -> Arch {
    Arch {
        my_pe: 0,
        num_pes: 1,
        pe_addrs: vec!["localhost".to_string()],
        job_id: 0,
    }
}
