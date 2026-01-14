# SIGSEGV: Segmentation Violation in zstd Decompression

## Issue Description

Oxia version 0.14.4 experienced segmentation violations during Pebble database read operations, specifically during zstd decompression of SSTable blocks.

### Stack Trace Pattern
```
SIGSEGV: segmentation violation
PC=0x7f16c4534c92 m=29 sigcode=128 addr=0x0
signal arrived during cgo execution

github.com/DataDog/zstd._Cfunc_ZSTD_decompress_wrapper
  ↓
github.com/DataDog/zstd.Decompress
  ↓
github.com/cockroachdb/pebble/sstable.decodeZstd
  ↓
github.com/cockroachdb/pebble/sstable.decompressInto
  ↓
github.com/cockroachdb/pebble/sstable.(*Reader).readBlock
  ↓
github.com/oxia-db/oxia/server/kv.(*Pebble).getFloor
```

## Root Cause

**This is NOT an Oxia internal resource leak.** This is a **known issue** in the interaction between:
- Pebble database versions < v1.1.0 (and some v1.x versions)
- DataDog/zstd library versions >= v1.5.2

### Technical Details

The issue was tracked as [Pebble Issue #1706](https://github.com/cockroachdb/pebble/issues/1706) from May 2022 to March 2024.

#### The Bug

1. **Buffer Size Requirements**: Starting with zstd v1.5.2, the `Decompress` function requires the destination buffer to be large enough to hold the **worst-case decoded size**, not just the actual decoded size. Pebble was allocating buffers using the exact size written in the block prefix.

2. **Buffer Allocation**: When the provided buffer was too small, zstd would allocate its own buffer instead of using the provided one. However, Pebble's manual memory management for the block cache required that zstd use the exact buffer provided.

3. **Error Handling**: Pebble was swallowing decompression errors and still returning the decoded buffer pointer, which could lead to:
   - Memory corruption
   - Use-after-free bugs  
   - Null pointer dereferences (addr=0x0 in the stack trace)
   - "Value already added to cache" panics

#### The Fix

The issue was resolved in [Pebble PR #3416](https://github.com/cockroachdb/pebble/pull/3416) (merged March 20, 2024):

1. Upgraded to zstd v1.5.6+
2. Changed to use the new `zstd.DecompressInto` API which guarantees decompression into the provided buffer
3. Fixed error handling to properly propagate decompression errors
4. Added proper validation and error checking

## Version Information

### Affected Versions

- **Pebble**: < v1.1.0 or v1.x with zstd >= v1.5.2
- **zstd**: v1.5.2 through v1.5.5 when used with older Pebble
- **Oxia**: v0.14.4 and earlier (depending on deployed Pebble/zstd versions)

### Fixed Versions

- **Pebble**: v2.0.0+ (fix merged in March 2024)
- **zstd**: v1.5.6+ (with DecompressInto API)
- **Oxia**: Current main branch uses Pebble v2.1.0 (Aug 2025) and zstd v1.5.7 ✅

## Diagnosis

If you encounter this crash, check:

1. **Binary Version**: Confirm the deployed binary matches the expected Pebble/zstd versions
   ```bash
   # Check go.mod versions
   cat go.mod | grep -E "pebble|zstd"
   
   # Or check the binary
   go version -m /path/to/oxia-binary | grep -E "pebble|zstd"
   ```

2. **Database Age**: Check if the database contains SSTables from older Pebble versions
   - Old SSTables may have been compressed with older zstd versions
   - The metadata format may differ from current expectations

3. **Crash Pattern**: Look for:
   - addr=0x0 in stack traces (null pointer)
   - Crashes during read-heavy workloads
   - Crashes during block cache eviction/reloading
   - "Value already added to cache" errors

## Resolution

### Option 1: Upgrade (Recommended)

Upgrade to a version of Oxia that includes:
- Pebble v2.0.0 or later
- zstd v1.5.6 or later

**Current Oxia main branch already has the fix** with Pebble v2.1.0 and zstd v1.5.7.

### Option 2: Database Compaction (For Legacy Data)

If you've upgraded but still see crashes, the database may contain old SSTables:

```go
// Force a full compaction to rewrite all SSTables with current format
err := db.Compact([]byte{0x00}, []byte{0xFF}, true /* parallelize */)
```

**Warning**: Full compaction is I/O intensive and should be done during a maintenance window.

### Option 3: Fresh Database

For development/testing environments, the safest approach is to start with a fresh database after upgrading.

## Prevention

1. **Keep Dependencies Updated**: Regularly update Pebble and zstd to latest stable versions
2. **Test Upgrades**: Test dependency upgrades in staging before production
3. **Monitor Versions**: Track which versions are deployed vs. in code
4. **Graceful Migration**: Plan for database format migrations during major version upgrades

## Related Issues

- [Pebble #1706](https://github.com/cockroachdb/pebble/issues/1706) - Main tracking issue
- [Pebble #3416](https://github.com/cockroachdb/pebble/pull/3416) - Fix PR
- [DataDog/zstd #130](https://github.com/DataDog/zstd/pull/130) - DecompressInto API addition

## Additional Resources

- [Pebble Release Notes](https://github.com/cockroachdb/pebble/releases)
- [DataDog/zstd Releases](https://github.com/DataDog/zstd/releases)
- [Oxia Release Notes](https://github.com/oxia-db/oxia/releases)

---

*Last updated: January 2026*
*Based on investigation of Oxia issue from version 0.14.4*
