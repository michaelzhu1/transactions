const mongoose = require('mongoose');

const Schema = mongoose.Schema;

const TransactionSchema = new Schema({
    involvesWatchonly: Boolean,
    account: String,
    address: String,
    category: String,
    amount: Schema.Types.Decimal128, 
    label: String,
    confirmations: Number,
    blockhash: String,
    blockindex: Number, 
    blocktime: Number, 
    txid: String,
    vout: Number,
    walletconflicts: Array,
    time: Number, 
    timereceived: Number,
    'bip125-replaceable': String
});

module.exports = mongoose.model('Transaction', TransactionSchema );