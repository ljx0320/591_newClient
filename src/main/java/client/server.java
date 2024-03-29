package client;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_HANDLER_RATIO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_AUXILIARY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_DEPTH;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_PATH_LENGTH;

import static org.apache.hadoop.util.Time.now;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSLimitException;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.RollingUpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeLifelineProtocolProtos.DatanodeLifelineProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos.DatanodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos.NamenodeProtocolService;
import org.apache.hadoop.hdfs.protocol.proto.ReconfigurationProtocolProtos.ReconfigurationProtocolService;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeLifelineProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ReconfigurationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HttpGetFailedException;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.FinalizeCommand;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RegisterCommand;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.GlobalStateIdContext;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RetryCache;
import org.apache.hadoop.ipc.RetryCache.CacheEntry;
import org.apache.hadoop.ipc.RetryCache.CacheEntryWithPayload;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.proto.RefreshAuthorizationPolicyProtocolProtos.RefreshAuthorizationPolicyProtocolService;
import org.apache.hadoop.security.proto.RefreshUserMappingsProtocolProtos.RefreshUserMappingsProtocolService;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshAuthorizationPolicyProtocolServerSideTranslatorPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolPB;
import org.apache.hadoop.security.protocolPB.RefreshUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolPB;
import org.apache.hadoop.ipc.protocolPB.RefreshCallQueueProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.proto.RefreshCallQueueProtocolProtos.RefreshCallQueueProtocolService;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolPB;
import org.apache.hadoop.ipc.protocolPB.GenericRefreshProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.proto.GenericRefreshProtocolProtos.GenericRefreshProtocolService;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.proto.GetUserMappingsProtocolProtos.GetUserMappingsProtocolService;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolPB;
import org.apache.hadoop.tools.protocolPB.GetUserMappingsProtocolServerSideTranslatorPB;
import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.apache.hadoop.tracing.TraceAdminPB.TraceAdminService;
import org.apache.hadoop.tracing.TraceAdminProtocolPB;
import org.apache.hadoop.tracing.TraceAdminProtocolServerSideTranslatorPB;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.slf4j.Logger;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.namenode.InotifyFSEditLogOpTranslator;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;

import javax.annotation.Nonnull;
public class server implements ClientProtocol {
    private FSNamesystem namesystem;
    private RetryCache retryCache;
    private Configuration conf;
    RPC.Server rpcServer;
    // constructor
    /*
    class updateImageThread extends Thread {
        public void run() {
            while (true) {
                try {
                    conf = new HdfsConfiguration();
                    conf.set("dfs.namenode.name.dir", "/hadoop/hdfs/name");
                    server.this.namesystem = FSNamesystem.loadFromDisk(conf);
                    server.this.retryCache = server.this.namesystem.getRetryCache();
                }
                catch(IOException e) {
                    server.this.namesystem.close();
                }
                try {
                    Thread.sleep(10000);
                }
                catch (InterruptedException e) {
                    server.this.namesystem.close();
                }
            }
        }
    }
    */
    public server() throws IOException{
        conf = new HdfsConfiguration();
        conf.set("dfs.namenode.name.dir", "/hadoop/hdfs/name");
        this.namesystem = FSNamesystem.loadFromDisk(conf);
        this.retryCache = namesystem.getRetryCache();

        ClientNamenodeProtocolServerSideTranslatorPB
                clientProtocolServerTranslator =
                new ClientNamenodeProtocolServerSideTranslatorPB(this);
        BlockingService clientNNPbService = ClientNamenodeProtocol.
                newReflectiveBlockingService(clientProtocolServerTranslator);
        GlobalStateIdContext stateIdContext = new GlobalStateIdContext((namesystem));

        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
                ProtobufRpcEngine.class);

        this.rpcServer = new RPC.Builder(conf)
                .setProtocol(
                        org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB.class)
                .setInstance(clientNNPbService).setBindAddress("127.0.0.1")
                .setPort(9001).setNumHandlers(1)
                .setVerbose(false)
                .setSecretManager(namesystem.getDelegationTokenSecretManager())
                .setAlignmentContext(stateIdContext)
                .build();
        // updateImageThread t1 = new updateImageThread();
        // t1.start();
/*
        NamenodeProtocolServerSideTranslatorPB namenodeProtocolXlator =
                new NamenodeProtocolServerSideTranslatorPB(this);
        BlockingService NNPbService = NamenodeProtocolService
                .newReflectiveBlockingService(namenodeProtocolXlator);
        RefreshUserMappingsProtocolServerSideTranslatorPB refreshUserMappingXlator =
                new RefreshUserMappingsProtocolServerSideTranslatorPB(this);
        BlockingService refreshUserMappingService = RefreshUserMappingsProtocolService
                .newReflectiveBlockingService(refreshUserMappingXlator);
        RefreshCallQueueProtocolServerSideTranslatorPB refreshCallQueueXlator =
                new RefreshCallQueueProtocolServerSideTranslatorPB(this);
        BlockingService refreshCallQueueService = RefreshCallQueueProtocolService
                .newReflectiveBlockingService(refreshCallQueueXlator);
        GenericRefreshProtocolServerSideTranslatorPB genericRefreshXlator =
                new GenericRefreshProtocolServerSideTranslatorPB(this);
        BlockingService genericRefreshService = GenericRefreshProtocolService
                .newReflectiveBlockingService(genericRefreshXlator);

        GetUserMappingsProtocolServerSideTranslatorPB getUserMappingXlator =
                new GetUserMappingsProtocolServerSideTranslatorPB(this);
        BlockingService getUserMappingService = GetUserMappingsProtocolService
                .newReflectiveBlockingService(getUserMappingXlator);
        TraceAdminProtocolServerSideTranslatorPB traceAdminXlator =
                new TraceAdminProtocolServerSideTranslatorPB(this);
        BlockingService traceAdminService = TraceAdminService
                .newReflectiveBlockingService(traceAdminXlator);

        DFSUtil.addPBProtocol(conf, NamenodeProtocolPB.class, NNPbService,
                server);
        DFSUtil.addPBProtocol(conf, RefreshUserMappingsProtocolPB.class,
                refreshUserMappingService, server);
        DFSUtil.addPBProtocol(conf, RefreshCallQueueProtocolPB.class,
                refreshCallQueueService, server);
        DFSUtil.addPBProtocol(conf, GenericRefreshProtocolPB.class,
                genericRefreshService, server);
        DFSUtil.addPBProtocol(conf, GetUserMappingsProtocolPB.class,
                getUserMappingService, server);
        DFSUtil.addPBProtocol(conf, TraceAdminProtocolPB.class,
                traceAdminService, server);

 */
    }

    void start() {
        this.rpcServer.start();
    }
    void join() throws InterruptedException {
        this.rpcServer.join();
    }
    void stop() {
        this.rpcServer.stop();
    }

    /* optimize ugi lookup for RPC operations to avoid a trip through
     * UGI.getCurrentUser which is synch'ed
     */
    public static UserGroupInformation getRemoteUser() throws IOException {
        UserGroupInformation ugi = Server.getRemoteUser();
        return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
    }

    @Override // ClientProtocol
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
            throws IOException {
        return namesystem.getDelegationToken(renewer);
    }

    @Override // ClientProtocol
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
            throws InvalidToken, IOException {
        return namesystem.renewDelegationToken(token);
    }

    @Override // ClientProtocol
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException {
        namesystem.cancelDelegationToken(token);
    }

    @Override // ClientProtocol
    public LocatedBlocks getBlockLocations(String src,
                                           long offset,
                                           long length)
            throws IOException {
        // metrics.incrGetBlockLocations();
        LocatedBlocks locatedBlocks =
                namesystem.getBlockLocations(getClientMachine(), src, offset, length);
        return locatedBlocks;
    }

    @Override // ClientProtocol
    public FsServerDefaults getServerDefaults() throws IOException {
        return namesystem.getServerDefaults();
    }

    @Override // ClientProtocol
    public HdfsFileStatus create(String src, FsPermission masked,
                                 String clientName, EnumSetWritable<CreateFlag> flag,
                                 boolean createParent, short replication, long blockSize,
                                 CryptoProtocolVersion[] supportedVersions)
            throws IOException {
        String clientMachine = getClientMachine();
        if (!checkPathLength(src)) {
            throw new IOException("create: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache, null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (HdfsFileStatus) cacheEntry.getPayload();
        }

        HdfsFileStatus status = null;
        try {
            PermissionStatus perm = new PermissionStatus(getRemoteUser()
                    .getShortUserName(), null, masked);
            status = namesystem.startFile(src, perm, clientName, clientMachine,
                    flag.get(), createParent, replication, blockSize, supportedVersions,
                    cacheEntry != null);
        } finally {
            RetryCache.setState(cacheEntry, status != null, status);
        }

        // metrics.incrFilesCreated();
        // metrics.incrCreateFileOps();
        return status;
    }

    @Override // ClientProtocol
    public LastBlockWithStatus append(String src, String clientName,
                                      EnumSetWritable<CreateFlag> flag) throws IOException {
        String clientMachine = getClientMachine();
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (LastBlockWithStatus) cacheEntry.getPayload();
        }

        LastBlockWithStatus info = null;
        boolean success = false;
        try {
            info = namesystem.appendFile(src, clientName, clientMachine, flag.get(),
                    cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success, info);
        }
        // metrics.incrFilesAppended();
        return info;
    }

    @Override // ClientProtocol
    public boolean recoverLease(String src, String clientName) throws IOException {
        String clientMachine = getClientMachine();
        return namesystem.recoverLease(src, clientName, clientMachine);
    }

    @Override // ClientProtocol
    public boolean setReplication(String src, short replication)
            throws IOException {
        return namesystem.setReplication(src, replication);
    }

    @Override
    public void unsetStoragePolicy(String src)
            throws IOException {
        namesystem.unsetStoragePolicy(src);
    }

    @Override
    public void setStoragePolicy(String src, String policyName)
            throws IOException {
        namesystem.setStoragePolicy(src, policyName);
    }

    @Override
    public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
        return namesystem.getStoragePolicy(path);
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return namesystem.getStoragePolicies();
    }

    @Override // ClientProtocol
    public void setPermission(String src, FsPermission permissions)
            throws IOException {
        namesystem.setPermission(src, permissions);
    }

    @Override // ClientProtocol
    public void setOwner(String src, String username, String groupname)
            throws IOException {
        namesystem.setOwner(src, username, groupname);
    }

    @Override
    public LocatedBlock addBlock(String src, String clientName,
                                 ExtendedBlock previous, DatanodeInfo[] excludedNodes, long fileId,
                                 String[] favoredNodes, EnumSet<AddBlockFlag> addBlockFlags)
            throws IOException {
        LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, fileId,
                clientName, previous, excludedNodes, favoredNodes, addBlockFlags);
        if (locatedBlock != null) {
            // metrics.incrAddBlockOps();
        }
        return locatedBlock;
    }

    @Override // ClientProtocol
    public LocatedBlock getAdditionalDatanode(final String src,
                                              final long fileId, final ExtendedBlock blk,
                                              final DatanodeInfo[] existings, final String[] existingStorageIDs,
                                              final DatanodeInfo[] excludes,
                                              final int numAdditionalNodes, final String clientName
    ) throws IOException {
        /*
        if (LOG.isDebugEnabled()) {
            LOG.debug("getAdditionalDatanode: src=" + src
                    + ", fileId=" + fileId
                    + ", blk=" + blk
                    + ", existings=" + Arrays.asList(existings)
                    + ", excludes=" + Arrays.asList(excludes)
                    + ", numAdditionalNodes=" + numAdditionalNodes
                    + ", clientName=" + clientName);
        }
        */
        // metrics.incrGetAdditionalDatanodeOps();

        Set<Node> excludeSet = null;
        if (excludes != null) {
            excludeSet = new HashSet<Node>(excludes.length);
            for (Node node : excludes) {
                excludeSet.add(node);
            }
        }
        LocatedBlock locatedBlock = namesystem.getAdditionalDatanode(src, fileId,
                blk, existings, existingStorageIDs, excludeSet, numAdditionalNodes,
                clientName);
        return locatedBlock;
    }
    /**
     * The client needs to give up on the block.
     */
    @Override // ClientProtocol
    public void abandonBlock(ExtendedBlock b, long fileId, String src,
                             String holder) throws IOException {
        namesystem.abandonBlock(b, fileId, src, holder);
    }

    @Override // ClientProtocol
    public boolean complete(String src, String clientName,
                            ExtendedBlock last,  long fileId)
            throws IOException {
        return namesystem.completeFile(src, clientName, last, fileId);
    }

    /**
     * The client has detected an error on the specified located blocks
     * and is reporting them to the server.  For now, the namenode will
     * mark the block as corrupt.  In the future we might
     * check the blocks are actually corrupt.
     */
    @Override // ClientProtocol, DatanodeProtocol
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
        namesystem.reportBadBlocks(blocks);
    }

    @Override // ClientProtocol
    public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
            throws IOException {
        return namesystem.updateBlockForPipeline(block, clientName);
    }


    @Override // ClientProtocol
    public void updatePipeline(String clientName, ExtendedBlock oldBlock,
                               ExtendedBlock newBlock, DatanodeID[] newNodes, String[] newStorageIDs)
            throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }

        boolean success = false;
        try {
            namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes,
                    newStorageIDs, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public long getPreferredBlockSize(String filename)
            throws IOException {
        return namesystem.getPreferredBlockSize(filename);
    }

    @Deprecated
    @Override // ClientProtocol
    public boolean rename(String src, String dst) throws IOException {
        if (!checkPathLength(dst)) {
            throw new IOException("rename: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return true; // Return previous response
        }

        boolean ret = false;
        try {
            ret = namesystem.renameTo(src, dst, cacheEntry != null);
        } finally {
            RetryCache.setState(cacheEntry, ret);
        }
        if (ret) {
            // metrics.incrFilesRenamed();
        }
        return ret;
    }

    @Override // ClientProtocol
    public void concat(String trg, String[] src) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;

        try {
            namesystem.concat(trg, src, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public void rename2(String src, String dst, Options.Rename... options)
            throws IOException {
        if (!checkPathLength(dst)) {
            throw new IOException("rename: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.renameTo(src, dst, cacheEntry != null, options);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
        // metrics.incrFilesRenamed();
    }

    @Override // ClientProtocol
    public boolean truncate(String src, long newLength, String clientName)
            throws IOException {
        String clientMachine = getClientMachine();
        try {
            return namesystem.truncate(
                    src, newLength, clientName, clientMachine, now());
        } finally {
            // metrics.incrFilesTruncated();
        }
    }

    @Override // ClientProtocol
    public boolean delete(String src, boolean recursive) throws IOException {

        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return true; // Return previous response
        }

        boolean ret = false;
        try {
            ret = namesystem.delete(src, recursive, cacheEntry != null);
        } finally {
            RetryCache.setState(cacheEntry, ret);
        }
        /*
        if (ret)
            metrics.incrDeleteFileOps();
         */
        return ret;
    }

    /**
     * Check path length does not exceed maximum.  Returns true if
     * length and depth are okay.  Returns false if length is too long
     * or depth is too great.
     */
    private boolean checkPathLength(String src) {
        Path srcPath = new Path(src);
        return (src.length() <= MAX_PATH_LENGTH &&
                srcPath.depth() <= MAX_PATH_DEPTH);
    }

    @Override // ClientProtocol
    public boolean mkdirs(String src, FsPermission masked, boolean createParent)
            throws IOException {
        if (!checkPathLength(src)) {
            throw new IOException("mkdirs: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        return namesystem.mkdirs(src,
                new PermissionStatus(getRemoteUser().getShortUserName(),
                        null, masked), createParent);
    }

    @Override // ClientProtocol
    public void renewLease(String clientName) throws IOException {
        namesystem.renewLease(clientName);
    }

    @Override // ClientProtocol
    public DirectoryListing getListing(String src, byte[] startAfter,
                                       boolean needLocation) throws IOException {
        DirectoryListing files = namesystem.getListing(
                src, startAfter, needLocation);
        if (files != null) {
            // metrics.incrGetListingOps();
            // metrics.incrFilesInGetListingOps(files.getPartialListing().length);
        }
        return files;
    }

    @Override // ClientProtocol
    public HdfsFileStatus getFileInfo(String src)  throws IOException {
        // metrics.incrFileInfoOps();
        return namesystem.getFileInfo(src, true);
    }

    @Override // ClientProtocol
    public boolean isFileClosed(String src) throws IOException{
        return namesystem.isFileClosed(src);
    }

    @Override // ClientProtocol
    public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
        // metrics.incrFileInfoOps();
        return namesystem.getFileInfo(src, false);
    }

    @Override // ClientProtocol
    public long[] getStats() throws IOException {
        namesystem.checkOperation(OperationCategory.READ);
        return namesystem.getStats();
    }

    @Override // ClientProtocol
    public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
            throws IOException {
        DatanodeInfo results[] = namesystem.datanodeReport(type);
        return results;
    }

    @Override // ClientProtocol
    public DatanodeStorageReport[] getDatanodeStorageReport(
            DatanodeReportType type) throws IOException {
        final DatanodeStorageReport[] reports = namesystem.getDatanodeStorageReport(type);
        return reports;
    }

    @Override // ClientProtocol
    public boolean setSafeMode(SafeModeAction action, boolean isChecked)
            throws IOException {
        OperationCategory opCategory = OperationCategory.UNCHECKED;
        if (isChecked) {
            if (action == SafeModeAction.SAFEMODE_GET) {
                opCategory = OperationCategory.READ;
            } else {
                opCategory = OperationCategory.WRITE;
            }
        }
        namesystem.checkOperation(opCategory);
        return namesystem.setSafeMode(action);
    }

    @Override // ClientProtocol
    public boolean restoreFailedStorage(String arg) throws IOException {
        return namesystem.restoreFailedStorage(arg);
    }

    @Override // ClientProtocol
    public void saveNamespace() throws IOException {
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.saveNamespace();
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public long rollEdits() throws AccessControlException, IOException {
        CheckpointSignature sig = namesystem.rollEditLog();
        return sig.getCurSegmentTxId();
    }

    @Override // ClientProtocol
    public void refreshNodes() throws IOException {
        namesystem.refreshNodes();
    }

    @Override // ClientProtocol
    public void finalizeUpgrade() throws IOException {
        namesystem.finalizeUpgrade();
    }

    @Override // ClientProtocol
    public RollingUpgradeInfo rollingUpgrade(RollingUpgradeAction action) throws IOException {
        // LOG.info("rollingUpgrade " + action);
        switch(action) {
            case QUERY:
                return namesystem.queryRollingUpgrade();
            case PREPARE:
                return namesystem.startRollingUpgrade();
            case FINALIZE:
                return namesystem.finalizeRollingUpgrade();
            default:
                throw new UnsupportedActionException(action + " is not yet supported.");
        }
    }

    @Override // ClientProtocol
    public void metaSave(String filename) throws IOException {
        namesystem.metaSave(filename);
    }

    @Override // ClientProtocol
    public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId)
            throws IOException {
        return namesystem.listOpenFiles(prevId);
    }

    @Override // ClientProtocol
    public void msync() throws IOException {
        // Check for write access to ensure that msync only happens on active
        namesystem.checkOperation(OperationCategory.WRITE);
    }

    @Override // ClientProtocol
    public HAServiceState getHAServiceState() throws IOException {
        // return nn.getServiceStatus().getState();
        return HAServiceState.OBSERVER;
    }

    @Override // ClientProtocol
    public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
            throws IOException {
        String[] cookieTab = new String[] { cookie };
        Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
                namesystem.listCorruptFileBlocks(path, cookieTab);

        String[] files = new String[fbs.size()];
        int i = 0;
        for(FSNamesystem.CorruptFileBlockInfo fb: fbs) {
            files[i++] = fb.path;
        }
        return new CorruptFileBlocks(files, cookieTab[0]);
    }

    /**
     * Tell all datanodes to use a new, non-persistent bandwidth value for
     * dfs.datanode.balance.bandwidthPerSec.
     * @param bandwidth Balancer bandwidth in bytes per second for all datanodes.
     * @throws IOException
     */
    @Override // ClientProtocol
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        namesystem.setBalancerBandwidth(bandwidth);
    }

    @Override // ClientProtocol
    public ContentSummary getContentSummary(String path) throws IOException {
        return namesystem.getContentSummary(path);
    }

    @Override // ClientProtocol
    public QuotaUsage getQuotaUsage(String path) throws IOException {
        return namesystem.getQuotaUsage(path);
    }

    @Override // ClientProtocol
    public void setQuota(String path, long namespaceQuota, long storagespaceQuota,
                         StorageType type)
            throws IOException {
        namesystem.setQuota(path, namespaceQuota, storagespaceQuota, type);
    }

    @Override // ClientProtocol
    public void fsync(String src, long fileId, String clientName,
                      long lastBlockLength)
            throws IOException {
        namesystem.fsync(src, fileId, clientName, lastBlockLength);
    }

    @Override // ClientProtocol
    public void setTimes(String src, long mtime, long atime)
            throws IOException {
        namesystem.setTimes(src, mtime, atime);
    }

    @Override // ClientProtocol
    public void createSymlink(String target, String link, FsPermission dirPerms,
                              boolean createParent) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }

        /* We enforce the MAX_PATH_LENGTH limit even though a symlink target
         * URI may refer to a non-HDFS file system.
         */
        if (!checkPathLength(link)) {
            throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                    " character limit");

        }

        final UserGroupInformation ugi = getRemoteUser();

        boolean success = false;
        try {
            PermissionStatus perm = new PermissionStatus(ugi.getShortUserName(),
                    null, dirPerms);
            namesystem.createSymlink(target, link, perm, createParent,
                    cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public String getLinkTarget(String path) throws IOException {
        // metrics.incrGetLinkTargetOps();
        HdfsFileStatus stat = null;
        try {
            stat = namesystem.getFileInfo(path, false);
        } catch (UnresolvedPathException e) {
            return e.getResolvedPath().toString();
        } catch (UnresolvedLinkException e) {
            // The NameNode should only throw an UnresolvedPathException
            throw new AssertionError("UnresolvedLinkException thrown");
        }
        if (stat == null) {
            throw new FileNotFoundException("File does not exist: " + path);
        } else if (!stat.isSymlink()) {
            throw new IOException("Path " + path + " is not a symbolic link");
        }
        return stat.getSymlink();
    }

    private void verifyRequest(NodeRegistration nodeReg) throws IOException {
        // verify registration ID
        final String id = nodeReg.getRegistrationID();
        final String expectedID = namesystem.getRegistrationID();
        if (!expectedID.equals(id)) {
            /*
            LOG.warn("Registration IDs mismatched: the "
                    + nodeReg.getClass().getSimpleName() + " ID is " + id
                    + " but the expected ID is " + expectedID);

             */
            throw new UnregisteredNodeException(nodeReg);
        }
    }

    /**
     * Verify version.
     * @param version layout version
     * @throws IOException on layout version mismatch
     */
    void verifyLayoutVersion(int version) throws IOException {
        if (version != HdfsServerConstants.NAMENODE_LAYOUT_VERSION)
            throw new IncorrectVersionException(
                    HdfsServerConstants.NAMENODE_LAYOUT_VERSION, version, "data node");
    }

    private void verifySoftwareVersion(DatanodeRegistration dnReg)
            throws IncorrectVersionException {
        String dnVersion = dnReg.getSoftwareVersion();
        /*
        if (VersionUtil.compareVersions(dnVersion, minimumDataNodeVersion) < 0) {
            IncorrectVersionException ive = new IncorrectVersionException(
                    minimumDataNodeVersion, dnVersion, "DataNode", "NameNode");
            // LOG.warn(ive.getMessage() + " DN: " + dnReg);
            throw ive;
        }
         */
        String nnVersion = VersionInfo.getVersion();
        if (!dnVersion.equals(nnVersion)) {
            String messagePrefix = "Reported DataNode version '" + dnVersion +
                    "' of DN " + dnReg + " does not match NameNode version '" +
                    nnVersion + "'";
            // long nnCTime = nn.getFSImage().getStorage().getCTime();
            long nnCTime = namesystem.getFSImage().getStorage().getCTime();
            long dnCTime = dnReg.getStorageInfo().getCTime();
            if (nnCTime != dnCTime) {
                IncorrectVersionException ive = new IncorrectVersionException(
                        messagePrefix + " and CTime of DN ('" + dnCTime +
                                "') does not match CTime of NN ('" + nnCTime + "')");
                // LOG.warn(ive.toString(), ive);
                throw ive;
            } else {
                // LOG.info(messagePrefix +
                   //     ". Note: This is normal during a rolling upgrade.");
            }
        }
    }

    private static String getClientMachine() {
        String clientMachine = Server.getRemoteAddress();
        if (clientMachine == null) { //not a RPC client
            clientMachine = "";
        }
        return clientMachine;
    }

    /**
     * Return the QOP of the client that the current handler thread
     * is handling. Assuming the negotiation is done at this point,
     * otherwise returns null.
     *
     * @return the established QOP of this client.
     */
    public static String getEstablishedClientQOP() {
        return Server.getEstablishedQOP();
    }

    @Override
    public DataEncryptionKey getDataEncryptionKey() throws IOException {
        return namesystem.getBlockManager().generateDataEncryptionKey();
    }

    @Override
    public String createSnapshot(String snapshotRoot, String snapshotName)
            throws IOException {
        if (!checkPathLength(snapshotRoot)) {
            throw new IOException("createSnapshot: Pathname too long.  Limit "
                    + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion(retryCache,
                null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (String) cacheEntry.getPayload();
        }

        // metrics.incrCreateSnapshotOps();
        String ret = null;
        try {
            ret = namesystem.createSnapshot(snapshotRoot, snapshotName,
                    cacheEntry != null);
        } finally {
            RetryCache.setState(cacheEntry, ret != null, ret);
        }
        return ret;
    }

    @Override
    public void deleteSnapshot(String snapshotRoot, String snapshotName)
            throws IOException {
        if (snapshotName == null || snapshotName.isEmpty()) {
            throw new IOException("The snapshot name is null or empty.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        // metrics.incrDeleteSnapshotOps();
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.deleteSnapshot(snapshotRoot, snapshotName, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override
    // Client Protocol
    public void allowSnapshot(String snapshotRoot) throws IOException {
        // metrics.incrAllowSnapshotOps();
        namesystem.allowSnapshot(snapshotRoot);
    }

    @Override
    // Client Protocol
    public void disallowSnapshot(String snapshot) throws IOException {
        // metrics.incrDisAllowSnapshotOps();
        namesystem.disallowSnapshot(snapshot);
    }

    @Override
    // ClientProtocol
    public void renameSnapshot(String snapshotRoot, String snapshotOldName,
                               String snapshotNewName) throws IOException {
        if (snapshotNewName == null || snapshotNewName.isEmpty()) {
            throw new IOException("The new snapshot name is null or empty.");
        }
        namesystem.checkOperation(OperationCategory.WRITE);
        // metrics.incrRenameSnapshotOps();
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.renameSnapshot(snapshotRoot, snapshotOldName,
                    snapshotNewName, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // Client Protocol
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing()
            throws IOException {
        SnapshottableDirectoryStatus[] status = namesystem
                .getSnapshottableDirListing();
        // metrics.incrListSnapshottableDirOps();
        return status;
    }

    @Override // ClientProtocol
    public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot,
                                                    String earlierSnapshotName, String laterSnapshotName) throws IOException {
        SnapshotDiffReport report = namesystem.getSnapshotDiffReport(snapshotRoot,
                earlierSnapshotName, laterSnapshotName);
        // metrics.incrSnapshotDiffReportOps();
        return report;
    }

    @Override // ClientProtocol
    public long addCacheDirective(
            CacheDirectiveInfo path, EnumSet<CacheFlag> flags) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntryWithPayload cacheEntry = RetryCache.waitForCompletion
                (retryCache, null);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return (Long) cacheEntry.getPayload();
        }

        boolean success = false;
        long ret = 0;
        try {
            ret = namesystem.addCacheDirective(path, flags, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success, ret);
        }
        return ret;
    }

    @Override // ClientProtocol
    public void modifyCacheDirective(
            CacheDirectiveInfo directive, EnumSet<CacheFlag> flags) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return;
        }

        boolean success = false;
        try {
            namesystem.modifyCacheDirective(directive, flags, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public void removeCacheDirective(long id) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return;
        }
        boolean success = false;
        try {
            namesystem.removeCacheDirective(id, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
                                                                   CacheDirectiveInfo filter) throws IOException {
        if (filter == null) {
            filter = new CacheDirectiveInfo.Builder().build();
        }
        return namesystem.listCacheDirectives(prevId, filter);
    }

    @Override //ClientProtocol
    public void addCachePool(CachePoolInfo info) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.addCachePool(info, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public void modifyCachePool(CachePoolInfo info) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.modifyCachePool(info, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public void removeCachePool(String cachePoolName) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return;
        }
        boolean success = false;
        try {
            namesystem.removeCachePool(cachePoolName, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public BatchedEntries<CachePoolEntry> listCachePools(String prevKey)
            throws IOException {
        return namesystem.listCachePools(prevKey != null ? prevKey : "");
    }

    @Override // ClientProtocol
    public void modifyAclEntries(String src, List<AclEntry> aclSpec)
            throws IOException {
        namesystem.modifyAclEntries(src, aclSpec);
    }

    @Override // ClienProtocol
    public void removeAclEntries(String src, List<AclEntry> aclSpec)
            throws IOException {
        namesystem.removeAclEntries(src, aclSpec);
    }

    @Override // ClientProtocol
    public void removeDefaultAcl(String src) throws IOException {
        namesystem.removeDefaultAcl(src);
    }

    @Override // ClientProtocol
    public void removeAcl(String src) throws IOException {
        namesystem.removeAcl(src);
    }

    @Override // ClientProtocol
    public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
        namesystem.setAcl(src, aclSpec);
    }

    @Override // ClientProtocol
    public AclStatus getAclStatus(String src) throws IOException {
        return namesystem.getAclStatus(src);
    }

    @Override // ClientProtocol
    public void createEncryptionZone(String src, String keyName)
            throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        final CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return;
        }
        boolean success = false;
        try {
            namesystem.createEncryptionZone(src, keyName, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public EncryptionZone getEZForPath(String src)
            throws IOException {
        return namesystem.getEZForPath(src);
    }

    @Override // ClientProtocol
    public BatchedEntries<EncryptionZone> listEncryptionZones(
            long prevId) throws IOException {
        return namesystem.listEncryptionZones(prevId);
    }

    @Override // ClientProtocol
    public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag)
            throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.setXAttr(src, xAttr, flag, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs)
            throws IOException {
        return namesystem.getXAttrs(src, xAttrs);
    }

    @Override // ClientProtocol
    public List<XAttr> listXAttrs(String src) throws IOException {
        return namesystem.listXAttrs(src);
    }

    @Override // ClientProtocol
    public void removeXAttr(String src, XAttr xAttr) throws IOException {
        namesystem.checkOperation(OperationCategory.WRITE);
        CacheEntry cacheEntry = RetryCache.waitForCompletion(retryCache);
        if (cacheEntry != null && cacheEntry.isSuccess()) {
            return; // Return previous response
        }
        boolean success = false;
        try {
            namesystem.removeXAttr(src, xAttr, cacheEntry != null);
            success = true;
        } finally {
            RetryCache.setState(cacheEntry, success);
        }
    }

    @Override // ClientProtocol
    public void checkAccess(String path, FsAction mode) throws IOException {
        namesystem.checkAccess(path, mode);
    }

    @Override // ClientProtocol
    public long getCurrentEditLogTxid() throws IOException {
        namesystem.checkOperation(OperationCategory.READ); // only active
        namesystem.checkSuperuserPrivilege();
        // if it's not yet open for write, we may be in the process of transitioning
        // from standby to active and may not yet know what the latest committed
        // txid is
        return namesystem.getEditLog().isOpenForWrite() ?
                namesystem.getEditLog().getLastWrittenTxId() : -1;
    }

    private static FSEditLogOp readOp(EditLogInputStream elis)
            throws IOException {
        try {
            return elis.readOp();
            // we can get the below two exceptions if a segment is deleted
            // (because we have accumulated too many edits) or (for the local journal/
            // no-QJM case only) if a in-progress segment is finalized under us ...
            // no need to throw an exception back to the client in this case
        } catch (FileNotFoundException e) {
            // LOG.debug("Tried to read from deleted or moved edit log segment", e);
            return null;
        } catch (HttpGetFailedException e) {
            // LOG.debug("Tried to read from deleted edit log segment", e);
            return null;
        }
    }

    @Override // ClientProtocol
    public EventBatchList getEditsFromTxid(final long txid) throws IOException {
        namesystem.checkOperation(OperationCategory.READ); // only active
        namesystem.checkSuperuserPrivilege();
        /*
        final int maxEventsPerRPC = nn.getConf().getInt(
                DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY,
                DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT);
         */
        final int maxEventsPerRPC = this.conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_KEY,
                DFSConfigKeys.DFS_NAMENODE_INOTIFY_MAX_EVENTS_PER_RPC_DEFAULT);
        final FSEditLog log = namesystem.getFSImage().getEditLog();
        final long syncTxid = log.getSyncTxId();
        // If we haven't synced anything yet, we can only read finalized
        // segments since we can't reliably determine which txns in in-progress
        // segments have actually been committed (e.g. written to a quorum of JNs).
        // If we have synced txns, we can definitely read up to syncTxid since
        // syncTxid is only updated after a transaction is committed to all
        // journals. (In-progress segments written by old writers are already
        // discarded for us, so if we read any in-progress segments they are
        // guaranteed to have been written by this NameNode.)
        final boolean readInProgress = syncTxid > 0;

        // doas the NN login user for the actual operations to get edits.
        // Notably this is necessary when polling from the remote edits via https.
        // We have validated the client is a superuser from the NN RPC, so this
        // running as the login user here is safe.
        EventBatchList ret = SecurityUtil.doAsLoginUser(
                new PrivilegedExceptionAction<EventBatchList>() {
                    @Override
                    public EventBatchList run() throws IOException {
                        return getEventBatchList(syncTxid, txid, log, readInProgress,
                                maxEventsPerRPC);
                    }
                });
        return ret;
    }

    private EventBatchList getEventBatchList(long syncTxid, long txid,
                                             FSEditLog log, boolean readInProgress, int maxEventsPerRPC)
            throws IOException {
        List<EventBatch> batches = Lists.newArrayList();
        int totalEvents = 0;
        long maxSeenTxid = -1;
        long firstSeenTxid = -1;

        if (syncTxid > 0 && txid > syncTxid) {
            // we can't read past syncTxid, so there's no point in going any further
            return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
        }

        Collection<EditLogInputStream> streams = null;
        try {
            streams = log.selectInputStreams(txid, 0, null, readInProgress);
        } catch (IllegalStateException e) { // can happen if we have
            // transitioned out of active and haven't yet transitioned to standby
            // and are using QJM -- the edit log will be closed and this exception
            // will result
            /*
            LOG.info("NN is transitioning from active to standby and FSEditLog " +
                    "is closed -- could not read edits");

             */
            return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
        }

        boolean breakOuter = false;
        for (EditLogInputStream elis : streams) {
            // our assumption in this code is the EditLogInputStreams are ordered by
            // starting txid
            try {
                FSEditLogOp op = null;
                while ((op = readOp(elis)) != null) {
                    // break out of here in the unlikely event that syncTxid is so
                    // out of date that its segment has already been deleted, so the first
                    // txid we get is greater than syncTxid
                    if (syncTxid > 0 && op.getTransactionId() > syncTxid) {
                        breakOuter = true;
                        break;
                    }

                    EventBatch eventBatch = InotifyFSEditLogOpTranslator.translate(op);
                    if (eventBatch != null) {
                        batches.add(eventBatch);
                        totalEvents += eventBatch.getEvents().length;
                    }
                    if (op.getTransactionId() > maxSeenTxid) {
                        maxSeenTxid = op.getTransactionId();
                    }
                    if (firstSeenTxid == -1) {
                        firstSeenTxid = op.getTransactionId();
                    }
                    if (totalEvents >= maxEventsPerRPC || (syncTxid > 0 &&
                            op.getTransactionId() == syncTxid)) {
                        // we're done
                        breakOuter = true;
                        break;
                    }
                }
            } finally {
                elis.close();
            }
            if (breakOuter) {
                break;
            }
        }
        return new EventBatchList(batches, firstSeenTxid, maxSeenTxid, syncTxid);
    }

}