package dev.brahmkshatriya.echo.playback.listener

import android.content.SharedPreferences
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import androidx.media3.common.Player.REPEAT_MODE_OFF
import androidx.media3.common.Timeline
import dev.brahmkshatriya.echo.common.Extension
import dev.brahmkshatriya.echo.common.MusicExtension
import dev.brahmkshatriya.echo.common.clients.AlbumClient
import dev.brahmkshatriya.echo.common.clients.PlaylistClient
import dev.brahmkshatriya.echo.common.clients.RadioClient
import dev.brahmkshatriya.echo.common.clients.SearchFeedClient
import dev.brahmkshatriya.echo.common.models.Album
import dev.brahmkshatriya.echo.common.models.EchoMediaItem
import dev.brahmkshatriya.echo.common.models.Feed.Companion.pagedDataOfFirst
import dev.brahmkshatriya.echo.common.models.Playlist
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.di.App
import dev.brahmkshatriya.echo.download.Downloader
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.get
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getAs
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getExtension
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getIf
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getOrThrow
import dev.brahmkshatriya.echo.extensions.MediaState
import dev.brahmkshatriya.echo.playback.MediaItemUtils
import dev.brahmkshatriya.echo.playback.MediaItemUtils.context
import dev.brahmkshatriya.echo.playback.PlayerCallback.Companion.resolvePlaybackExtensions
import dev.brahmkshatriya.echo.playback.MediaItemUtils.extensionId
import dev.brahmkshatriya.echo.playback.MediaItemUtils.track
import dev.brahmkshatriya.echo.playback.PlayerState
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicInteger

class PlayerRadio(
    private val app: App,
    private val scope: CoroutineScope,
    private val player: Player,
    private val throwFlow: MutableSharedFlow<Throwable>,
    private val stateFlow: MutableStateFlow<PlayerState.Radio>,
    private val extensionList: StateFlow<List<MusicExtension>>,
    private val downloadFlow: StateFlow<List<Downloader.Info>>
) : Player.Listener {

    private val radioGeneration = AtomicInteger(0)

    // Tracks playlist/album continuation so we can load more from the same source
    private var playlistContinuation: String? = null
    private var playlistContextId: String? = null

    companion object {
        const val AUTO_START_RADIO = "auto_start_radio"
        const val RADIO_EXTENSION_ID = "radio_extension_id"
        suspend fun start(
            throwableFlow: MutableSharedFlow<Throwable>,
            extension: Extension<*>,
            item: EchoMediaItem,
            itemContext: EchoMediaItem?
        ): PlayerState.Radio.Loaded? {
            if (!item.isRadioSupported) return null
            return extension.getIf<RadioClient, PlayerState.Radio.Loaded?> {
                val radio = radio(item, itemContext)
                val tracks = loadTracks(radio).pagedDataOfFirst()
                PlayerState.Radio.Loaded(extension.id, radio, null) {
                    extension.get { tracks.loadPage(it) }.getOrThrow(throwableFlow)
                }
            }.getOrThrow(throwableFlow)
        }

        suspend fun play(
            player: Player,
            downloadFlow: StateFlow<List<Downloader.Info>>,
            app: App,
            stateFlow: MutableStateFlow<PlayerState.Radio>,
            loaded: PlayerState.Radio.Loaded
        ) {
            stateFlow.value = PlayerState.Radio.Loading
            val tracks = loaded.tracks(loaded.cont) ?: return

            stateFlow.value = if (tracks.continuation == null) PlayerState.Radio.Empty
            else loaded.copy(cont = tracks.continuation)

            val playbackExtIds = app.resolvePlaybackExtensions(loaded.clientId)
            val proxyIds = if (playbackExtIds.singleOrNull() == loaded.clientId) emptyList()
            else playbackExtIds

            val item = tracks.data.map {
                MediaItemUtils.build(
                    app,
                    downloadFlow.value,
                    MediaState.Unloaded(loaded.clientId, it),
                    loaded.context,
                    proxyIds
                )
            }

            val addedCount = withContext(Dispatchers.Main) {
                val existingIds = mutableSetOf<String>()
                val existingNames = mutableSetOf<String>()
                for (i in 0 until player.mediaItemCount) {
                    runCatching { player.getMediaItemAt(i).track }.getOrNull()?.let { track ->
                        existingIds.add(track.id)
                        existingNames.add("${track.title.trim()} ${track.artists.firstOrNull()?.name?.trim().orEmpty()}".lowercase())
                    }
                }

                val newItems = item.filter { mediaItem ->
                    val track = mediaItem.track
                    val name = "${track.title.trim()} ${track.artists.firstOrNull()?.name?.trim().orEmpty()}".lowercase()
                    !existingIds.contains(track.id) && !existingNames.contains(name)
                }

                if (newItems.isNotEmpty()) {
                    player.addMediaItems(newItems)
                    player.prepare()
                }
                newItems.size
            }

            if (addedCount == 0 && stateFlow.value is PlayerState.Radio.Loaded) {
                play(player, downloadFlow, app, stateFlow, stateFlow.value as PlayerState.Radio.Loaded)
            }
        }
    }

    private suspend fun loadPlaylist() {
        val gen = radioGeneration.get()
        val mediaItem = withContext(Dispatchers.Main) { player.currentMediaItem } ?: return
        val radioExtId = app.settings.getString(RADIO_EXTENSION_ID, null)?.takeIf { it.isNotBlank() }
        val originalExtId = mediaItem.extensionId
        val extensionId = radioExtId ?: originalExtId
        val originalTrack = mediaItem.track
        val itemContext = mediaItem.context
        stateFlow.value = PlayerState.Radio.Loading
        val extension = extensionList.getExtension(extensionId) ?: return

        // If the current track belongs to a playlist/album, try loading more from it first
        if (itemContext != null && (itemContext is Playlist || itemContext is Album)) {
            val added = tryLoadMoreFromContext(extension, itemContext, originalExtId, gen)
            if (added) return // Successfully added more playlist/album tracks
            // Playlist/album exhausted — fall through to radio
        }

        // If using a different extension for radio, search for the track on that extension first
        val item: EchoMediaItem = if (radioExtId != null && radioExtId != originalExtId) {
            searchTrackOnExtension(extension, originalTrack) ?: originalTrack
        } else {
            originalTrack
        }

        // If generation changed, a new song started playing — discard this stale load
        if (radioGeneration.get() != gen) return

        val loaded = start(throwFlow, extension, item, itemContext)

        if (radioGeneration.get() != gen) return
        stateFlow.value = loaded ?: PlayerState.Radio.Empty
        if (loaded != null) play(player, downloadFlow, app, stateFlow, loaded)
    }

    /**
     * Try to load more tracks from the same playlist/album context.
     * Returns true if new tracks were added, false if the source is exhausted.
     */
    private suspend fun tryLoadMoreFromContext(
        extension: Extension<*>,
        context: EchoMediaItem,
        extensionId: String,
        gen: Int
    ): Boolean {
        // If context changed (different playlist), reset continuation
        if (playlistContextId != context.id) {
            playlistContextId = context.id
            playlistContinuation = null
        }

        // null continuation with a known context means we haven't tracked it yet —
        // build the paged data and skip already-queued pages to find the continuation
        val pagedData = when (context) {
            is Playlist -> extension.getAs<PlaylistClient, _> {
                loadTracks(loadPlaylist(context)).pagedDataOfFirst()
            }.getOrNull()
            is Album -> extension.getAs<AlbumClient, _> {
                loadTracks(loadAlbum(context))?.pagedDataOfFirst()
            }.getOrNull()
            else -> null
        } ?: return false

        // Collect existing track IDs in queue for dedup
        val existingIds = mutableSetOf<String>()
        val existingNames = mutableSetOf<String>()
        withContext(Dispatchers.Main) {
            for (i in 0 until player.mediaItemCount) {
                runCatching { player.getMediaItemAt(i).track }.getOrNull()?.let { track ->
                    existingIds.add(track.id)
                    existingNames.add(
                        "${track.title.trim()} ${track.artists.firstOrNull()?.name?.trim().orEmpty()}".lowercase()
                    )
                }
            }
        }

        // Load pages until we find new tracks or exhaust the source
        var continuation = playlistContinuation
        val newTracks = mutableListOf<Track>()

        // Load up to 3 pages at a time
        var pagesLoaded = 0
        while (pagesLoaded < 3) {
            val page = extension.get { pagedData.loadPage(continuation) }.getOrNull() ?: break
            if (radioGeneration.get() != gen) return false

            for (track in page.data) {
                val name = "${track.title.trim()} ${track.artists.firstOrNull()?.name?.trim().orEmpty()}".lowercase()
                if (!existingIds.contains(track.id) && !existingNames.contains(name)) {
                    newTracks.add(track)
                    existingIds.add(track.id)
                    existingNames.add(name)
                }
            }
            continuation = page.continuation
            pagesLoaded++
            if (continuation == null) break // No more pages
        }

        playlistContinuation = continuation

        if (newTracks.isEmpty()) {
            // Source exhausted, no new tracks found
            stateFlow.value = PlayerState.Radio.Empty
            return false
        }

        val playbackExtIds = app.resolvePlaybackExtensions(extensionId)
        val proxyIds = if (playbackExtIds.singleOrNull() == extensionId) emptyList()
        else playbackExtIds

        val mediaItems = newTracks.map {
            MediaItemUtils.build(
                app,
                downloadFlow.value,
                MediaState.Unloaded(extensionId, it),
                context,
                proxyIds
            )
        }

        withContext(Dispatchers.Main) {
            player.addMediaItems(mediaItems)
            player.prepare()
        }
        // Keep state as Empty so when these new tracks run out, we'll come back here again
        stateFlow.value = if (continuation != null) PlayerState.Radio.Empty
        else PlayerState.Radio.Empty
        return true
    }

    private suspend fun searchTrackOnExtension(
        extension: Extension<*>,
        original: Track
    ): Track? {
        val artist = original.artists.firstOrNull()?.name.orEmpty().trim()
        val query = if (artist.isBlank()) original.title
        else "${original.title} $artist"

        return extension.getAs<SearchFeedClient, Track?> {
            val feed = loadSearchFeed(query)
            val data = feed.getPagedData(null)
            val tracks = data.pagedData.loadPage(null).data.flatMap { shelf ->
                when (shelf) {
                    is Shelf.Item -> listOfNotNull(shelf.media as? Track)
                    is Shelf.Lists.Tracks -> shelf.list
                    is Shelf.Lists.Items -> shelf.list.filterIsInstance<Track>()
                    else -> emptyList()
                }
            }
            tracks.firstOrNull()
        }.getOrNull()
    }

    private var autoStartRadio = app.settings.getBoolean(AUTO_START_RADIO, true)

    private val listener = SharedPreferences.OnSharedPreferenceChangeListener { pref, key ->
        if (key != AUTO_START_RADIO) return@OnSharedPreferenceChangeListener
        autoStartRadio = pref.getBoolean(AUTO_START_RADIO, true)
    }

    init {
        app.settings.registerOnSharedPreferenceChangeListener(listener)
    }

    private suspend fun startRadio() {
        if (!autoStartRadio) return
        val shouldNotStart = withContext(Dispatchers.Main) {
            player.run {
                currentMediaItem == null || repeatMode != REPEAT_MODE_OFF || hasNextMediaItem()
            }
        }
        if (shouldNotStart) return
        when (val state = stateFlow.value) {
            is PlayerState.Radio.Loading -> {}
            is PlayerState.Radio.Empty -> loadPlaylist()
            is PlayerState.Radio.Loaded -> play(player, downloadFlow, app, stateFlow, state)
        }
    }

    override fun onTimelineChanged(timeline: Timeline, reason: Int) {
        scope.launch { startRadio() }
    }

    override fun onMediaItemTransition(mediaItem: MediaItem?, reason: Int) {
        // Reset radio state so fresh radio/playlist loading is triggered for new context
        stateFlow.value = PlayerState.Radio.Empty
        // If the context changed (different playlist/album), reset continuation tracking
        val newContextId = mediaItem?.let { runCatching { it.context?.id }.getOrNull() }
        if (newContextId != playlistContextId) {
            playlistContinuation = null
            playlistContextId = newContextId
        }
        // Bump generation so any in-flight radio loads from the previous song are discarded
        radioGeneration.incrementAndGet()
        scope.launch { startRadio() }
    }
}

