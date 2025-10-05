#[macro_export]
macro_rules! ratio {
    ($numer:expr, $denom:expr) => {{
        let numer = num_bigint::BigInt::from($numer);
        let denom = num_bigint::BigInt::from($denom);
        num_rational::BigRational::new(numer, denom)
    }};
    ($x:expr) => {{
        let x = num_bigint::BigInt::from($x);
        num_rational::BigRational::from_integer(x)
    }};
}

#[macro_export]
macro_rules! floor_int {
    ($x:expr, $ty:ty) => {
        <$ty>::try_from($x.floor().to_integer()).unwrap()
    };
}

#[macro_export]
macro_rules! pallas_ratio {
    ($x:expr) => {{
        $crate::ratio!($x.numerator, $x.denominator)
    }};
}
