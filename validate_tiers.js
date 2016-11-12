// @flow

module.exports = function(tiers/*:TiersType*/)/*:ValidationType*/ {
    if (Array.isArray(tiers)) {
        return true;
    }
    else {
        return false;
    }
};
