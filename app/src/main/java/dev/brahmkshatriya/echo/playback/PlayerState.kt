package dev.brahmkshatriya.echo.playback

import androidx.media3.common.MediaItem
import dev.brahmkshatriya.echo.common.helpers.Page
import dev.brahmkshatriya.echo.common.models.EchoMediaItem
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.playback.MediaItemUtils.context
import dev.brahmkshatriya.echo.playback.MediaItemUtils.track
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow

data class PlayerState(
    val current: MutableStateFlow<Current?> = MutableStateFlow(null),
    val radio: MutableStateFlow<Radio> = MutableStateFlow(Radio.Empty),
    val session: MutableStateFlow<Int> = MutableStateFlow(0)
) {

    val servers: MutableMap<String, Result<Streamable.Media.Server>> =
        object : LinkedHashMap<String, Result<Streamable.Media.Server>>(64, 0.75f, true) {
            override fun removeEldestEntry(
                eldest: MutableMap.MutableEntry<String, Result<Streamable.Media.Server>>?
            ) = size > 100
        }
    val serverChanged = MutableSharedFlow<Unit>()

    data class Current(
        val index: Int,
        val mediaItem: MediaItem,
        val isLoaded: Boolean,
        val isPlaying: Boolean,
    ) {

        val context by lazy { mediaItem.context }
        val track by lazy { mediaItem.track }
        fun isPlaying(id: String?): Boolean {
            val same = mediaItem.mediaId == id
                    || context?.id == id
                    || track.album?.id == id
                    || track.artists.any { it.id == id }
            return isPlaying && same
        }

        companion object {
            fun Current?.isPlaying(id: String?): Boolean = this?.isPlaying(id) ?: false
        }
    }

    sealed class Radio {
        data object Empty : Radio()
        data object Loading : Radio()
        data class Loaded(
            val clientId: String,
            val context: EchoMediaItem,
            val cont: String?,
            val tracks: suspend (String?) -> Page<Track>?
        ) : Radio()
    }
}
