use std::collections::HashSet;

use rand::Rng;

use crate::generation::{pick, readable_name_custom, Arbitrary};
use crate::model::table::{Column, ColumnType, Name, Table};

impl Arbitrary for Name {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = readable_name_custom("_", rng);
        Name(name.replace("-", "_"))
    }
}

impl Arbitrary for Table {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let columns = loop {
            let columns = (1..=rng.gen_range(1..10))
                .map(|_| Column::arbitrary(rng))
                .collect::<Vec<_>>();
            // TODO: see if there is a better way to detect duplicates here
            let mut set = HashSet::with_capacity(columns.len());
            set.extend(columns.iter());
            // Has repeated column name inside so generate again
            if set.len() != columns.len() {
                continue;
            }
            break columns;
        };

        Table {
            rows: Vec::new(),
            name,
            columns,
        }
    }
}

impl Arbitrary for Column {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        let name = Name::arbitrary(rng).0;
        let column_type = ColumnType::arbitrary(rng);
        Self {
            name,
            column_type,
            primary: false,
            unique: false,
        }
    }
}

impl Arbitrary for ColumnType {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        pick(&[Self::Integer, Self::Float, Self::Text, Self::Blob], rng).to_owned()
    }
}
