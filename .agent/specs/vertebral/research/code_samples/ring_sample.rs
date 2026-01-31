pub struct SpscRing {
    region: Arc<ShmRegion>,
    header: *mut ShmHeader,
    data: *mut u8,
    layout: RingLayout,
}

impl SpscRing {
    pub unsafe fn initialize(region: Arc<ShmRegion>, layout: RingLayout) -> Result<Self, ChannelError> {
        // ... initialization logic ...
    }

    pub fn try_send(&self, payload: &[u8], flags: u32) -> Result<(), ChannelError> {
        // ... lock-free send logic ...
    }

    pub fn recv_hybrid(&self, out: &mut [u8], spin_us: u64, timeout: Duration) -> Result<Option<usize>, ChannelError> {
        // ... hybrid spin/park receive logic ...
    }
}
