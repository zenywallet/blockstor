function Marker(opts, libs) {
    var db = libs.db;

    this.rollbacking = false;

    this.rollback_markers = async function(sequence) {
        var markers = await db.getMarkers();
        for(var i in markers) {
            if(markers[i].sequence > sequence) {
                await db.setMarker(markers[i].apikey, sequence);
            }
        }
    }

    this.delete_unused_markers = async function() {
        var markers = await db.getMarkers();
        for(var i in markers) {
            var apikey = markers[i].apikey;
            if(!opts.apikeys[apikey]) {
                await db.delMarker(apikey);
            }
        }
    }

    this.delete_all_markers = async function() {
        var markers = await db.getMarkers();
        for(var i in markers) {
            await db.delMarker(markers[i].apikey);
        }
    }
}

module.exports = Marker;
