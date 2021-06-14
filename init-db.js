var fPools = [{"name": "RUNE-ETH3", "addr": "0x3139bbba7f4b9125595cb4ebeefdac1fce7ab5f1"},
          {"name": "MATIC-ETH3", "addr": "0x290a6a7460b308ee3f19023d2d00de604bcf5b42"}];

var j;
for (j=0;j<fPools.length;j++){
  db.pools.insert({"name": fPools[j].name,"addr": fPools[j].addr,"last_block": 12617470});
  db.createCollection(fPools[j].name, capped=false);
}
