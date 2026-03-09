package dev.brahmkshatriya.echo.analytics

import dev.brahmkshatriya.echo.analytics.db.PlayHistoryDao
import dev.brahmkshatriya.echo.analytics.db.PlayHistoryEntity
import dev.brahmkshatriya.echo.common.models.ImageHolder
import dev.brahmkshatriya.echo.di.App
import dev.brahmkshatriya.echo.playback.MediaItemUtils.extensionId
import dev.brahmkshatriya.echo.playback.MediaItemUtils.track
import dev.brahmkshatriya.echo.playback.PlayerState
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch

class PlayHistoryRecorder(
    app: App,
    private val dao: PlayHistoryDao,
    playerState: PlayerState,
) {
    init {
        app.scope.launch(Dispatchers.IO) {
            var currentRowId: Long? = null
            var startTime = 0L
            var maxDuration = 0L

            playerState.current
                .map { it?.takeIf { c -> c.isLoaded }?.mediaItem }
                .distinctUntilChanged()
                .collect { mediaItem ->
                    // Update previous track's duration with actual elapsed time
                    currentRowId?.let { id ->
                        val elapsed = System.currentTimeMillis() - startTime
                        val actualDuration = minOf(elapsed, maxDuration)
                        if (actualDuration > 2000) {
                            dao.updateDuration(id, actualDuration)
                        }
                    }
                    currentRowId = null

                    mediaItem ?: return@collect

                    val track = mediaItem.track
                    val now = System.currentTimeMillis()
                    startTime = now
                    maxDuration = track.duration ?: (30 * 60 * 1000L)

                    val coverUrl = when (val cover = track.cover) {
                        is ImageHolder.NetworkRequestImageHolder -> cover.request.url
                        is ImageHolder.ResourceUriImageHolder -> cover.uri
                        else -> null
                    }

                    // Insert immediately so plays/songs show up right away
                    currentRowId = dao.insert(
                        PlayHistoryEntity(
                            trackId = track.id,
                            trackTitle = track.title,
                            artistNames = track.artists.joinToString(", ") { it.name },
                            albumName = track.album?.title,
                            albumId = track.album?.id,
                            coverUrl = coverUrl,
                            duration = 0,
                            extensionId = mediaItem.extensionId,
                            timestamp = now
                        )
                    )
                }
        }
    }
}
