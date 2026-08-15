pub mod pointers {
    use pallas::ledger::{addresses::Pointer, primitives::StakeCredential};
    use tracing::warn;

    pub fn pointer_to_cred(pointer: &Pointer) -> Option<StakeCredential> {
        match (pointer.slot(), pointer.tx_idx(), pointer.cert_idx()) {
            // preview
            (2940289, 1, 0) => Some(StakeCredential::AddrKeyhash(
                "0c90492bbe7eb33f38173255e547dc3194abcec5cd29cdf504bb4f03"
                    .parse()
                    .unwrap(),
            )),
            (100, 2, 0) => None,
            (1, 1, 1) => None,
            (0, 0, 0) => None,
            (0, 1, 10000) => None,
            (10000000, 1, 1) => None,
            (100, 100, 1) => None,
            (1, 1, 1000) => None,
            (1, 1, 0) => None,
            (50, 50, 5) => None,

            // preprod
            (10612742, 0, 0) => Some(StakeCredential::AddrKeyhash(
                "4dcca876aac2fcc561f7df3da772d747e2148c9a05c7b27e49a05ea2"
                    .parse()
                    .unwrap(),
            )),
            (70549345, 1, 0) => Some(StakeCredential::AddrKeyhash(
                "b1a3b1ef9460dc7bef8ffdf49ce4e01b1cc2505c614ee62b3223f458"
                    .parse()
                    .unwrap(),
            )),
            (82626550, 0, 0) => None,
            (2498243, 27, 3) => None,

            // mainnet
            (4495800, 11, 0) => Some(StakeCredential::AddrKeyhash(
                "bc1597ad71c55d2d009a9274b3831ded155118dd769f5376decc1369"
                    .parse()
                    .unwrap(),
            )),
            (20095460, 2, 0) => Some(StakeCredential::AddrKeyhash(
                "1332d859dd71f5b1089052a049690d81f7367eac9fafaef80b4da395"
                    .parse()
                    .unwrap(),
            )),

            // Add all unmapped pointers from analysis as None
            (12, 12, 12) => None,
            (62, 96, 105) => None,
            (116, 49, 0) => None,
            (124, 21, 3807) => None,
            (13005, 15312, 1878946283) => None,
            (13200, 526450, 149104513) => None,
            (222624, 45784521, 167387965) => None,
            (105, 13146, 24) => None,
            (16292793057, 1011302, 20) => None,
            (18446744073709551615, 1221092, 2) => Some(StakeCredential::AddrKeyhash(
                "1332d859dd71f5b1089052a049690d81f7367eac9fafaef80b4da395"
                    .parse()
                    .unwrap(),
            )),
            (53004562, 9, 0) => Some(StakeCredential::AddrKeyhash(
                "e46c33afa9ca60cfeb3b7452a415c271772020b3f57ac90c496a6127"
                    .parse()
                    .unwrap(),
            )),
            (156960568, 15, 0) => Some(StakeCredential::AddrKeyhash(
                "a3d3ba720c11bb6b7364bb0ee2abfca79ec135aaafe0bd0b89f24121"
                    .parse()
                    .unwrap(),
            )),
            (78312587, 5, 0) => Some(StakeCredential::AddrKeyhash(
                "a773914d934899b3656f7f4edc3293c5804dc288faa468f6587f05e6"
                    .parse()
                    .unwrap(),
            )),

            (slot, tx_idx, cert_idx) => {
                warn!(slot, tx_idx, cert_idx, "missing pointer mapping");
                panic!()
            }
        }
    }
}
