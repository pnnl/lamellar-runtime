use crate::array::{LamellarByteArray,ScalarType};



/// Runtime tag for the 7 builtin reduction operations.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum BuiltinOp {
    Sum,
    Prod,
    Max,
    Min,
    And,
    Or,
    Xor,
}


#[crabtime::function]
fn impl_reduce_scalar_type_match(){
    let mut reduce_match_arms = Vec::new();
    let mut merge_match_arms = Vec::new();
    let arithmetic_ops = crabtime::quote! {
        BuiltinOp::Sum  => |a, b| a + b,
        BuiltinOp::Prod => |a, b| a * b,
        BuiltinOp::Max  => |a, b| if a > b { a } else { b },
        BuiltinOp::Min  => |a, b| if a < b { a } else { b },
    };
    let bitwise_ops = crabtime::quote! {
        BuiltinOp::And  => |a, b| a & b,
        BuiltinOp::Or   => |a, b| a | b,
        BuiltinOp::Xor  => |a, b| a ^ b,
    };
    for ty in ["u8", "u16", "u32", "u64", "usize", "u128", "i8", "i16", "i32", "i64", "isize", "i128","bool","f32","f64"].iter() {
        let mut op_arms = crabtime::quote!{  };
        if *ty == "bool" {
            op_arms = crabtime::quote!{
                {{bitwise_ops}}
            };
        } else if *ty == "f32" || *ty == "f64" {
            op_arms = crabtime::quote!{
                {{arithmetic_ops}}
            };
        } else {
            op_arms = crabtime::quote!{
                {{arithmetic_ops}}
                {{bitwise_ops}}
            };
        }
        reduce_match_arms.push(crabtime::quote!{
            ScalarType::{{ty}} => {
                let local = self.data.local_data::<{{ty}}>().await;
                let closure: fn({{ty}}, {{ty}}) -> {{ty}} = match self.op {
                    {{op_arms}}
                    _ => panic!("reduce operation not supported"),
                };
                match local.reduce(closure) {
                    Some(v) => crate::serialize(&v, false).expect("reduce_scalar_local ser res"),
                    None => Vec::new(),
                }
            }
        });
        merge_match_arms.push(crabtime::quote!{
            ScalarType::{{ty}} => {
                let left = crate::deserialize::<{{ty}}>(&left,false).expect("merge_scalar des left");
                let right = crate::deserialize::<{{ty}}>(&right,false).expect("merge_scalar des right");
                let closure: fn({{ty}}, {{ty}}) -> {{ty}} = match self.op {
                    {{op_arms}}
                    _ => panic!("merge operation not supported" ),
                };
                let res = closure(left, right);
                crate::serialize(&res, false).expect("merge_scalar ser res")
            }
        });
    }
    let reduce_match_arms = reduce_match_arms.join("\n");
    let merge_match_arms = merge_match_arms.join("\n");

    crabtime::output!{
        async fn reduce_scalar_local(&self) -> Vec<u8> {
            match self.scalar_type {
                {{reduce_match_arms}}
            }
        }

        async fn merge_scalar(
            &self,
            left: Vec<u8>,
            right: Vec<u8>,
        ) -> Vec<u8> {
            if left.is_empty() && right.is_empty() {
                Vec::new()
            } else if left.is_empty() {
                right
            } else if right.is_empty() {
                left
            } else {
                match self.scalar_type {
                    {{merge_match_arms}}
                }
            }
        }
    }
}

/// Single AM struct used for ALL builtin reductions on ALL primitive types.
/// The array type is erased into `LamellarByteArray`; `scalar_type` and `op` tag
/// which concrete type and operation to dispatch at runtime.
#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone)]
pub(crate) struct ScalarBuiltinReductionAm {
    pub data: LamellarByteArray,
    pub start_pe: usize,
    pub end_pe: usize,
    pub scalar_type: ScalarType,
    pub op: BuiltinOp,
}

impl ScalarBuiltinReductionAm {
    impl_reduce_scalar_type_match!();
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarBuiltinReductionAm {

    

    async fn exec(&self) -> Vec<u8> {
        if self.start_pe == self.end_pe {
            self.reduce_scalar_local().await
        } else {
            let mid_pe = (self.start_pe + self.end_pe) / 2;
            let left = __lamellar_team.spawn_am_pe(self.start_pe, ScalarBuiltinReductionAm {
                data: self.data.clone(),
                start_pe: self.start_pe,
                end_pe: mid_pe,
                scalar_type: self.scalar_type,
                op: self.op,
            });
            let right = __lamellar_team.spawn_am_pe(mid_pe + 1, ScalarBuiltinReductionAm {
                data: self.data.clone(),
                start_pe: mid_pe + 1,
                end_pe: self.end_pe,
                scalar_type: self.scalar_type,
                op: self.op,
            });
            let left_bytes = left.await;
            let right_bytes = right.await;
            self.merge_scalar(left_bytes, right_bytes).await
        }
    }
}

impl ScalarBuiltinReductionAm {
    pub(crate) fn new(
        data: LamellarByteArray,
        scalar_type: ScalarType,
        op: BuiltinOp,
    ) -> Self {
        let num_pes = data.team().num_pes()-1;
        Self {
            data,
            start_pe: 0,
            end_pe: num_pes,
            scalar_type,
            op,
        }
    }
}
