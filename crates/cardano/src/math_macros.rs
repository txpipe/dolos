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
macro_rules! ibig {
    ($x:expr) => {{
        num_bigint::BigInt::from($x)
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

#[macro_export]
macro_rules! add {
    ($a:expr, $b:expr) => {{
        #[cfg(feature = "relaxed")]
        {
            $a.saturating_add($b)
        }
        #[cfg(not(feature = "relaxed"))]
        {
            $a.checked_add($b).expect("overflow in strict mode")
        }
    }};
}

#[macro_export]
macro_rules! sub {
    ($a:expr, $b:expr) => {{
        #[cfg(feature = "relaxed")]
        {
            $a.saturating_sub($b)
        }
        #[cfg(not(feature = "relaxed"))]
        {
            $a.checked_sub($b).expect("overflow in strict mode")
        }
    }};
}
