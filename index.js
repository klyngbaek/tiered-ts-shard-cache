//@flow

"use strict";

var DataShard = require('ts-shard-cache');

class TieredTsShardCache {

    /*::_tiers: TiersType*/
    /*::_dataShards: Array<DataShard>*/

    constructor(tiers/*:TiersType*/, fetchFunc/*:TieredFetchFuncType*/, handleFetch/*:?HandleFetchFuncType*/)/*:void*/ {
        this._tiers = tiers;
        this._dataShards = tiers.map(function(shardSize/*:ShardSizeType*/, tier/*:TierType*/)/*:DataShard*/ {
            function tieredFetchFunc(range/*RangeType:*/, cb/*:FetchCallbackType*/)/*:void*/ {
                fetchFunc(range, tier, cb);
            }
            return new DataShard(shardSize, tieredFetchFunc, handleFetch);
        });
    }

    // fetch

    fetchMissingDataForRange(range/*:RangeType*/, tier/*:TierType*/)/*:void*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        this._dataShards[tier].fetchMissingDataForRange(range);
    }

    fetchDataForShardIds(shardIds/*:Array<ShardIdType>*/, tier/*:TierType*/)/*:void*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        this._dataShards[tier].fetchDataForShardIds(shardIds);
    }

    fetchDataForShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:void*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        this._dataShards[tier].fetchDataForShardId(shardId);
    }

    // set

    setDataForShardId(shardId/*:ShardIdType*/, tier/*:TierType*/, data/*:Array<DataPoint>*/)/*:void*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        this._dataShards[tier].setDataForShardId(shardId, data);
    }

    // add

    addDataPoint(dataPoint/*:DataPoint*/, tier/*:TierType*/)/*:void*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        this._dataShards[tier].addDataPoint(dataPoint);
    }

    // get

    getDataForShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:Array<DataPoint>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].getDataForShardId(shardId);
    }

    getDataForShardIds(shardIds/*:Array<ShardIdType>*/, tier/*:TierType*/)/*:Array<DataPoint>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].getDataForShardIds(shardIds);
    }

    getDataForRange(range/*:RangeType*/, tier/*:TierType*/)/*:Array<DataPoint>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].getDataForRange(range);
    }

    // compute

    computeMissingShardIdsFromShardIds(shardIds/*:Array<ShardIdType>*/, tier/*:TierType*/)/*:Array<ShardIdType>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].computeMissingShardIdsFromShardIds(shardIds);
    }

    computeMissingShardIdsFromRange(range/*:RangeType*/, tier/*:TierType*/)/*:Array<ShardIdType>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].computeMissingShardIdsFromRange(range);
    }

    computeShardIdsFromRange(range/*:RangeType*/, tier/*:TierType*/)/*:Array<ShardIdType>*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].computeShardIdsFromRange(range);
    }

    computeRangeFromShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:RangeType*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].computeRangeFromShardId(shardId);
    }

    // has

    hasDataForShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:boolean*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].hasDataForShardId(shardId);
    }

    hasPendingReqForShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:boolean*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].hasPendingReqForShardId(shardId);
    }

    // validate

    validateShardId(shardId/*:ShardIdType*/, tier/*:TierType*/)/*:ValidationType*/ {
        if (!validateTier(this._tiers, tier)) throw new Error('Invalid input tier: ' + tier);
        return this._dataShards[tier].validateShardId(shardId);
    }

    validateShardData(shardId/*:ShardIdType*/, tier/*:TierType*/, shardData/*:Array<DataPoint>*/)/*:ValidationType*/ {
        return this._dataShards[tier].validateShardData(shardId, shardData);
    }

}

// validate

function validateTier(tiers/*:TiersType*/, tier/*:TierType*/)/*:ValidationType*/ {
    if (typeof tier !== 'number') return false;
    if (tier < 0 || tier >= tiers.length) return false;
    return true;
}

module.exports = TieredTsShardCache;
