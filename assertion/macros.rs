#[doc(hidden)]
#[macro_export]
macro_rules! valueset {

    // === base case ===
    (@ { $(,)* $($val:expr),* $(,)* }, $next:expr $(,)*) => {
        &[ $($val),* ]
    };

    // === recursive case (more tts) ===

    // TODO(#1138): determine a new syntax for uninitialized span fields, and
    // re-enable this.
    // (@{ $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = _, $($rest:tt)*) => {
    //     $crate::valueset!(@ { $($out),*, (&$next, None) }, $next, $($rest)*)
    // };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = ?$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = %$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = $val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$val as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$($k).+ as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, ?$($k:ident).+, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$($k).+) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, %$($k:ident).+, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$($k).+) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = ?$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = %$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+ = $val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$val as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $($k:ident).+) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$($k).+ as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, ?$($k:ident).+) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$($k).+) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, %$($k:ident).+) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$($k).+) as &dyn Value)) },
            $next,
        )
    };

    // Handle literal names
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = ?$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = %$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = $val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$val as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = ?$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&debug(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = %$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&display(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, $k:literal = $val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, core::option::Option::Some(&$val as &dyn Value)) },
            $next,
        )
    };

    // Handle constant names
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = ?$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&debug(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = %$val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&display(&$val) as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = $val:expr, $($rest:tt)*) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&$val as &dyn Value)) },
            $next,
            $($rest)*
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = ?$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&debug(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = %$val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&display(&$val) as &dyn Value)) },
            $next,
        )
    };
    (@ { $(,)* $($out:expr),* }, $next:expr, { $k:expr } = $val:expr) => {
        $crate::valueset!(
            @ { $($out),*, (&$next, Some(&$val as &dyn Value)) },
            $next,
        )
    };

    // Remainder is unparsable, but exists --- must be format args!
    (@ { $(,)* $($out:expr),* }, $next:expr, $($rest:tt)+) => {
        $crate::valueset!(@ { (&$next, core::option::Option::Some(&core::format_args!($($rest)+) as &dyn Value)), $($out),* }, $next, )
    };

    // === entry ===
    ($fields:expr, $($kvs:tt)+) => {
        {
            #[allow(unused_imports)]
            use $crate::field::{debug, display, Value};
            let mut iter = $fields.iter();
            $fields.value_set($crate::valueset!(
                @ { },
                core::iter::Iterator::next(&mut iter).expect("FieldSet corrupted (this is a bug)"),
                $($kvs)+
            ))
        }
    };
    ($fields:expr,) => {
        {
            $fields.value_set(&[])
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! fieldset {
    // == base case ==
    (@ { $(,)* $($out:expr),* $(,)* } $(,)*) => {
        {
            const FIELDS: &[&str] = &[ $($out),* ];
            FIELDS
        }
    };

    // == recursive cases (more tts) ==
    (@ { $(,)* $($out:expr),* } $($k:ident).+ = ?$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } $($k:ident).+ = %$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } $($k:ident).+ = $val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };
    // TODO(#1138): determine a new syntax for uninitialized span fields, and
    // re-enable this.
    // (@ { $($out:expr),* } $($k:ident).+ = _, $($rest:tt)*) => {
    //     $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    // };
    (@ { $(,)* $($out:expr),* } ?$($k:ident).+, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } %$($k:ident).+, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } $($k:ident).+, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $crate::__assert_stringify!($($k).+) } $($rest)*)
    };

    // Handle literal names
    (@ { $(,)* $($out:expr),* } $k:literal = ?$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } $k:literal = %$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } $k:literal = $val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };

    // Handle constant names
    (@ { $(,)* $($out:expr),* } { $k:expr } = ?$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } { $k:expr } = %$val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };
    (@ { $(,)* $($out:expr),* } { $k:expr } = $val:expr, $($rest:tt)*) => {
        $crate::fieldset!(@ { $($out),*, $k } $($rest)*)
    };

    // Remainder is unparsable, but exists --- must be format args!
    (@ { $(,)* $($out:expr),* } $($rest:tt)+) => {
        $crate::fieldset!(@ { "message", $($out),*, })
    };

    // == entry ==
    ($($args:tt)*) => {
        $crate::fieldset!(@ { } $($args)*,)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __assert_stringify {
    ($($k:ident).+) => {{
        const NAME: $crate::FieldName<{
            $crate::FieldName::len(core::stringify!($($k).+))
        }> = $crate::FieldName::new(core::stringify!($($k).+));
        NAME.as_str()
    }};
}

#[macro_export]
macro_rules! event {
    ({ $($fields:tt)* } )=> ({
        $crate::valueset!($crate::field::FieldSet::new($crate::fieldset!( $($fields)* )), $($fields)*)
    });
    ({ $($fields:tt)* }, $($arg:tt)+ ) => (
        $crate::event!(
            { message = core::format_args!($($arg)+), $($fields)* }
        )
    );
    ($($k:ident).+ = $($fields:tt)* ) => (
        $crate::event!({ $($k).+ = $($fields)* })
    );
    ($($arg:tt)+) => (
        $crate::event!({ $($arg)+ })
    );
    (?$($k:ident).+ = $($field:tt)*) => (
        $crate::event!({ ?$($k).+ = $($field)*})
    );
    (%$($k:ident).+ = $($field:tt)*) => (
        $crate::event!({ %$($k).+ = $($field)*})
    );
    ($($k:ident).+, $($field:tt)*) => (
        $crate::event!({ $($k).+, $($field)*})
    );
    (%$($k:ident).+, $($field:tt)*) => (
        $crate::event!({ %$($k).+, $($field)*})
    );
    (?$($k:ident).+, $($field:tt)*) => (
        $crate::event!({ ?$($k).+, $($field)*})
    );
    ($($k:ident).+ = $($field:tt)*) => (
        $crate::event!({ $($k).+ = $($field)*})
    );
}

#[cfg(test)]
mod tests {

    #[test]
    fn test1() {
        let val = 1;
        let test = 10;

        let x = event!(val);
        let y = event!(test);
        dbg!(&x);
    }
}
