package dev.brahmkshatriya.echo.analytics.db

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "play_history")
data class PlayHistoryEntity(
    @PrimaryKey(autoGenerate = true) val id: Long = 0,
    val trackId: String,
    val trackTitle: String,
    val artistNames: String,
    val albumName: String?,
    val albumId: String?,
    val coverUrl: String?,
    val duration: Long?,
    val extensionId: String,
    val timestamp: Long
)
