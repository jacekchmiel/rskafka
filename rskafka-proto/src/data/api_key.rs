use rskafka_wire_format::{
    error::{custom_error, ParseError},
    prelude::*,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
}

impl ApiKey {
    pub fn try_from_i16(v: i16) -> Option<Self> {
        use ApiKey::*;
        match v {
            0 => Some(Produce),
            1 => Some(Fetch),
            2 => Some(ListOffsets),
            3 => Some(Metadata),
            4 => Some(LeaderAndIsr),
            5 => Some(StopReplica),
            6 => Some(UpdateMetadata),
            7 => Some(ControlledShutdown),
            8 => Some(OffsetCommit),
            9 => Some(OffsetFetch),
            10 => Some(FindCoordinator),
            11 => Some(JoinGroup),
            12 => Some(Heartbeat),
            13 => Some(LeaveGroup),
            14 => Some(SyncGroup),
            15 => Some(DescribeGroups),
            16 => Some(ListGroups),
            17 => Some(SaslHandshake),
            18 => Some(ApiVersions),
            19 => Some(CreateTopics),
            20 => Some(DeleteTopics),
            21 => Some(DeleteRecords),
            22 => Some(InitProducerId),
            23 => Some(OffsetForLeaderEpoch),
            24 => Some(AddPartitionsToTxn),
            25 => Some(AddOffsetsToTxn),
            26 => Some(EndTxn),
            27 => Some(WriteTxnMarkers),
            28 => Some(TxnOffsetCommit),
            29 => Some(DescribeAcls),
            30 => Some(CreateAcls),
            31 => Some(DeleteAcls),
            32 => Some(DescribeConfigs),
            33 => Some(AlterConfigs),
            34 => Some(AlterReplicaLogDirs),
            35 => Some(DescribeLogDirs),
            36 => Some(SaslAuthenticate),
            37 => Some(CreatePartitions),
            38 => Some(CreateDelegationToken),
            39 => Some(RenewDelegationToken),
            40 => Some(ExpireDelegationToken),
            41 => Some(DescribeDelegationToken),
            42 => Some(DeleteGroups),
            43 => Some(ElectLeaders),
            44 => Some(IncrementalAlterConfigs),
            45 => Some(AlterPartitionReassignments),
            46 => Some(ListPartitionReassignments),
            47 => Some(OffsetDelete),
            _ => None,
        }
    }

    pub fn to_i16(&self) -> i16 {
        *self as i16
    }
}

impl WireFormatParse for ApiKey {
    fn parse(input: &[u8]) -> IResult<&[u8], Self, ParseError> {
        let (input, value) = i16::parse(input)?;
        match ApiKey::try_from_i16(value) {
            Some(api_key) => Ok((input, api_key)),
            None => Err(custom_error("unknown api key")),
        }
    }
}

impl WireFormatSizeStatic for ApiKey {
    fn wire_size_static() -> usize {
        i16::wire_size_static()
    }
}

impl WireFormatWrite for ApiKey {
    fn wire_size(&self) -> usize {
        Self::wire_size_static()
    }

    fn write_into<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_i16().write_into(writer)
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}({})", self, self.to_i16())
    }
}
