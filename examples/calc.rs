use std::num::ParseIntError;

use redis_module::{key::RedisKey, redis_module, Context, RedisError, RedisResult, RedisString, RedisValue};

struct CommonArgs {
    a: i64,
    b: i64,
    out_id: RedisString,
}

fn integer_from_key2(name: &str,key: RedisKey) -> RedisResult<i64> {
    //key.get_value::<RedisString>()

    let res = key
        .read()?
        .map_or(RedisValue::SimpleString(format!("Not found {}", name)), |v| RedisValue::StringBuffer(Vec::from(v)));
    
    match res {
        RedisValue::Null => Err(RedisError::Str("Null")),
        RedisValue::Integer(x) => Ok(x),
        RedisValue::StringBuffer(buf) => {
            String::from_utf8(buf.into())
                .map_err(|_| RedisError::Str("No utf8"))?
                .parse()
                .map_err(|e: ParseIntError | RedisError::String(format!("{}", e)))
        },
        RedisValue::BulkRedisString(x) => {
            x.parse_integer()
        },
        RedisValue::BulkString(s) | RedisValue::SimpleString(s) => {
            s.parse().map_err(|e: ParseIntError | RedisError::String(format!("{} '{}'", e, s)))
        },
        RedisValue::SimpleStringStatic(s) => {
            s.parse().map_err(|e: ParseIntError | RedisError::String(format!("{} '{}'", e, s)))
        }
        _ => Err(RedisError::Str("Wrong Type")),
    }
}

fn parse_args(ctx: &Context, args: Vec<RedisString>) -> RedisResult<CommonArgs> {
    if args.len() != 4 {
        return Err(RedisError::String(format!("Args={} but should be =4", args.len())));
    }

    let mut it = args.into_iter().skip(1);
    let a = it.next()
        .ok_or(RedisError::Str("Could not parse a"))
        .map(|s|     
            integer_from_key2(s.try_as_str().unwrap(), ctx.open_key(&s))
        )??;

    let b =  it.next()
        .ok_or(RedisError::Str("Could not parse b"))
        .map(|s|     
            integer_from_key2(s.try_as_str().unwrap(), ctx.open_key(&s))
        )??;
    
    let out_id = it.next().unwrap();

    Ok(CommonArgs { a, b, out_id: out_id})
}


fn binary_arithmethic_body<F: FnOnce(i64, i64) -> i64>(
    ctx: &Context, 
    args: Vec<RedisString>, 
    ftor: F) -> RedisResult<RedisValue> {

    // 1. get values over keys
    let args = parse_args(ctx, args)?;

    // 2. operation:
    let res = ftor(args.a, args.b);
    let res = res.to_string();

    // 2, write value to out key
    let out_key = ctx.open_key_writable(&args.out_id);
    out_key.write(res.as_str())?;

    Ok(RedisValue::SimpleStringStatic("OK"))
}

fn calc_add(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    binary_arithmethic_body(ctx, args, |a, b | a+b)
}

fn calc_sub(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    binary_arithmethic_body(ctx, args, |a, b | a-b)
}

fn calc_mul(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    binary_arithmethic_body(ctx, args, |a, b | a*b)
}

fn calc_div(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    binary_arithmethic_body(ctx, args, |a, b | a/b)
}

//////////////////////////////////////////////////////

redis_module! {
    name: "calc",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    commands: [
        ["calc.add", calc_add, "", 0, 0, 0, ""],
        ["calc.sub", calc_sub, "", 0, 0, 0, ""],
        ["calc.mul", calc_mul, "", 0, 0, 0, ""],
        ["calc.div", calc_div, "", 0, 0, 0, ""],
    ],
}
