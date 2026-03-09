package dev.brahmkshatriya.echo.analytics.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query

data class ArtistStat(val artistNames: String, val coverUrl: String?, val playCount: Int)
data class TrackStat(
    val trackId: String,
    val trackTitle: String,
    val artistNames: String,
    val coverUrl: String?,
    val extensionId: String,
    val playCount: Int
)
data class AlbumStat(val albumName: String, val albumId: String?, val coverUrl: String?, val playCount: Int)

@Dao
interface PlayHistoryDao {

    @Insert
    suspend fun insert(entry: PlayHistoryEntity): Long

    @Query("UPDATE play_history SET duration = :duration WHERE id = :id")
    suspend fun updateDuration(id: Long, duration: Long)

    @Query(
        "SELECT artistNames, coverUrl, COUNT(*) AS playCount FROM play_history " +
                "WHERE timestamp >= :since GROUP BY artistNames ORDER BY playCount DESC LIMIT :limit"
    )
    suspend fun getTopArtists(since: Long, limit: Int = 50): List<ArtistStat>

    @Query(
        "SELECT trackId, trackTitle, artistNames, coverUrl, extensionId, COUNT(*) AS playCount FROM play_history " +
                "WHERE timestamp >= :since GROUP BY trackId ORDER BY playCount DESC LIMIT :limit OFFSET :offset"
    )
    suspend fun getTopTracks(since: Long, limit: Int = 20, offset: Int = 0): List<TrackStat>

    @Query("SELECT COALESCE(SUM(COALESCE(duration, 0)), 0) FROM play_history WHERE timestamp >= :since")
    suspend fun getTotalListeningTime(since: Long): Long

    @Query(
        "SELECT albumName, albumId, coverUrl, COUNT(*) AS playCount FROM play_history " +
                "WHERE timestamp >= :since AND albumName IS NOT NULL " +
                "GROUP BY albumId ORDER BY playCount DESC LIMIT :limit"
    )
    suspend fun getTopAlbums(since: Long, limit: Int = 50): List<AlbumStat>

    @Query("SELECT COUNT(*) FROM play_history WHERE timestamp >= :since")
    suspend fun getTotalPlays(since: Long): Int
}
