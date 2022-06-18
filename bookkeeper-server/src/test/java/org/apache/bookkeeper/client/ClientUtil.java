package org.apache.bookkeeper.client;

//IT ORIGINALLY WAS AN APACHE-BOOKKEEPER TEST CLASS

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.security.GeneralSecurityException;
import java.util.function.Function;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.versioning.Versioned;

/**
 * Client utilities.
 */
public class ClientUtil {
    public static final org.apache.bookkeeper.client.api.DigestType DIGEST_TYPE =
            org.apache.bookkeeper.client.api.DigestType.CRC32C;
    public static final byte[] PASSWD = "foobar".getBytes(UTF_8);

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed,
                                         long length, byte[] data) throws GeneralSecurityException {
        return generatePacket(ledgerId, entryId, lastAddConfirmed, length, data, 0, data.length);
    }

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed, long length, byte[] data,
                                         int offset, int len) throws GeneralSecurityException {
        DigestManager dm = DigestManager.instantiate(ledgerId, new byte[2], DigestType.CRC32,
                UnpooledByteBufAllocator.DEFAULT, true);
        return ByteBufList.coalesce(dm.computeDigestAndPackageForSending(entryId, lastAddConfirmed, length,
                Unpooled.wrappedBuffer(data, offset, len)));
    }

    /**
     * Returns that whether ledger is in open state.
     */
    public static boolean isLedgerOpen(LedgerHandle handle) {
        return !handle.getLedgerMetadata().isClosed();
    }

    public static Versioned<LedgerMetadata> setupLedger(ClientContext clientCtx, long ledgerId,
                                                        LedgerMetadataBuilder builder) throws Exception {
        return setupLedger(clientCtx.getLedgerManager(), ledgerId, builder);
    }

    public static Versioned<LedgerMetadata> setupLedger(LedgerManager ledgerManager, long ledgerId,
                                                        LedgerMetadataBuilder builder) throws Exception {
        LedgerMetadata md = builder.withPassword(PASSWD).withDigestType(DIGEST_TYPE).withId(ledgerId).build();
        return ledgerManager.createLedgerMetadata(ledgerId, md).get();
    }

    public static Versioned<LedgerMetadata> transformMetadata(ClientContext clientCtx, long ledgerId,
                                                              Function<LedgerMetadata, LedgerMetadata> transform)
            throws Exception {
        return transformMetadata(clientCtx.getLedgerManager(), ledgerId, transform);
    }

    public static Versioned<LedgerMetadata> transformMetadata(LedgerManager ledgerManager, long ledgerId,
                                                              Function<LedgerMetadata, LedgerMetadata> transform)
            throws Exception {
        Versioned<LedgerMetadata> current = ledgerManager.readLedgerMetadata(ledgerId).get();
        return ledgerManager.writeLedgerMetadata(ledgerId, transform.apply(current.getValue()),
                current.getVersion()).get();
    }

}