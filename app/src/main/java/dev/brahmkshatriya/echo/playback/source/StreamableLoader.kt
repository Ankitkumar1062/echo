package dev.brahmkshatriya.echo.playback.source

import android.net.Uri
import androidx.media3.common.MediaItem
import dev.brahmkshatriya.echo.common.Extension
import dev.brahmkshatriya.echo.common.MusicExtension
import dev.brahmkshatriya.echo.common.clients.SearchFeedClient
import dev.brahmkshatriya.echo.common.models.Feed
import dev.brahmkshatriya.echo.common.models.Shelf
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.common.models.Streamable.Source.Companion.toSource
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.di.App
import dev.brahmkshatriya.echo.download.Downloader
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getAs
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getExtension
import dev.brahmkshatriya.echo.extensions.ExtensionUtils.getExtensionOrThrow
import dev.brahmkshatriya.echo.extensions.MediaState
import dev.brahmkshatriya.echo.extensions.cache.Cached
import dev.brahmkshatriya.echo.extensions.cache.Cached.loadStreamableMedia
import dev.brahmkshatriya.echo.playback.MediaItemUtils
import dev.brahmkshatriya.echo.playback.MediaItemUtils.backgroundIndex
import dev.brahmkshatriya.echo.playback.MediaItemUtils.downloaded
import dev.brahmkshatriya.echo.playback.MediaItemUtils.extensionId
import dev.brahmkshatriya.echo.playback.MediaItemUtils.isLoaded
import dev.brahmkshatriya.echo.playback.MediaItemUtils.proxyExtensionIds
import dev.brahmkshatriya.echo.playback.MediaItemUtils.serverIndex
import dev.brahmkshatriya.echo.playback.MediaItemUtils.state
import dev.brahmkshatriya.echo.playback.MediaItemUtils.subtitleIndex
import dev.brahmkshatriya.echo.playback.MediaItemUtils.track
import dev.brahmkshatriya.echo.ui.media.MediaHeaderAdapter.Companion.playableString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.text.Normalizer
import kotlin.math.abs
import kotlin.math.min

class StreamableLoader(
    private val app: App,
    private val extensionListFlow: StateFlow<List<MusicExtension>>,
    private val downloadFlow: StateFlow<List<Downloader.Info>>
) {
    suspend fun load(mediaItem: MediaItem) = withContext(Dispatchers.IO) {
        extensionListFlow.first { it.isNotEmpty() }
        val new = if (mediaItem.isLoaded) mediaItem
        else MediaItemUtils.buildLoaded(
            app, downloadFlow.value, mediaItem, loadTrack(mediaItem)
        )

        val server = async { loadServer(new) }
        val background =
            async { if (new.backgroundIndex < 0) null else loadBackground(new).getOrNull() }
        val subtitle = async { if (new.subtitleIndex < 0) null else loadSubtitle(new).getOrNull() }

        MediaItemUtils.buildWithBackgroundAndSubtitle(
            new, background.await(), subtitle.await()
        ) to server.await()
    }

    private suspend fun <T> withClient(
        mediaItem: MediaItem,
        block: suspend (Extension<*>) -> Result<T>
    ): Result<T> {
        val extension = extensionListFlow.getExtensionOrThrow(mediaItem.extensionId)
        return block(extension)
    }

    private suspend fun loadTrack(item: MediaItem): MediaState.Loaded<Track> {
        val isProxy = hasSpotifyRefs(item.track)

        // For non-proxy tracks, try the normal load path first.
        if (!isProxy) {
            val result = withClient(item) { extension ->
                Cached.loadMedia(app, extension, item.state)
            }
            if (result.isSuccess && !hasSpotifyRefs(result.getOrThrow().item)) {
                return result.getOrThrow()
            }
        }

        // Fast path: check the in-memory cache for a previously resolved mapping.
        val cacheKey = "${item.extensionId}:${item.track.id}"
        val cached = resolvedCache[cacheKey]
        if (cached != null) {
            val (cachedTrack, cachedExtId) = cached
            val cachedExt = extensionListFlow.getExtension(cachedExtId)
            if (cachedExt != null) {
                val loaded = runCatching {
                    Cached.loadMedia(
                        app, cachedExt,
                        MediaState.Unloaded(cachedExtId, cachedTrack)
                    ).getOrThrow()
                }
                if (loaded.isSuccess && !hasSpotifyRefs(loaded.getOrThrow().item)) {
                    return loaded.getOrThrow()
                }
            }
            resolvedCache.remove(cacheKey)
        }

        // Search-based resolution. Use multi-extension fan-out if configured.
        val proxyIds = item.proxyExtensionIds
        val extensions = if (proxyIds.size > 1) {
            coroutineScope {
                proxyIds.map { id -> async { extensionListFlow.getExtension(id) } }
                    .mapNotNull { it.await() }
            }
        } else null

        return if (extensions != null && extensions.isNotEmpty()) {
            // ── Multi-extension: search + load in streaming pipeline ──────────
            resolveAndLoadMulti(extensions, item.track, cacheKey)
        } else {
            // ── Single-extension path (legacy / fallback) ─────────────────────
            withClient(item) { extension ->
                runCatching {
                    val candidates = resolveTrackBySearch(extension, item.track).getOrThrow()
                    val tagged = candidates.map { it to extension }
                    loadBestTaggedCandidate(cacheKey, tagged)
                }
            }.getOrThrow()
        }
    }

    /**
     * Combined search + load pipeline for multi-extension mode.
     *
     * Searches all [extensions] in parallel.
     * - Fast path: if any extension returns an *exact* match (ISRC or title+artist+duration),
     *   load it immediately and cancel remaining searches.
     * - Otherwise: wait for ALL extensions to finish, merge + globally rank candidates by
     *   [scoreMatch], then load the best ones in order.
     */
    private suspend fun resolveAndLoadMulti(
        extensions: List<Extension<*>>,
        original: Track,
        cacheKey: String,
    ): MediaState.Loaded<Track> = coroutineScope {
        val resultChannel = Channel<List<Pair<Track, Extension<*>>>>(capacity = extensions.size)

        // Launch parallel searches.
        val searchJobs = extensions.map { ext ->
            launch {
                val found = resolveTrackBySearch(ext, original).getOrElse { emptyList() }
                if (found.isNotEmpty()) {
                    resultChannel.send(found.take(5).map { it to ext })
                }
            }
        }

        // Close channel when all searches are done.
        launch {
            searchJobs.forEach { it.join() }
            resultChannel.close()
        }

        // Collect batches as they arrive; short-circuit ONLY on exact match.
        val allTagged = mutableListOf<Pair<Track, Extension<*>>>()

        for (batch in resultChannel) {
            val exactHit = batch.firstOrNull { isExactMatch(original, it.first) }
            if (exactHit != null) {
                // Exact match found — try loading it immediately.
                val (candidate, sourceExt) = exactHit
                val loaded = runCatching {
                    Cached.loadMedia(
                        app, sourceExt,
                        MediaState.Unloaded(sourceExt.id, candidate)
                    ).getOrThrow()
                }
                if (loaded.isSuccess && !hasSpotifyRefs(loaded.getOrThrow().item)) {
                    resolvedCache[cacheKey] = candidate to sourceExt.id
                    searchJobs.forEach { it.cancel() }
                    return@coroutineScope loaded.getOrThrow()
                }
                // Exact match failed to load — fall through to collect all results.
            }
            allTagged.addAll(batch)
        }

        // No exact match loaded. Globally rank all collected candidates and load best.
        val sorted = allTagged
            .distinctBy { it.first.id }
            .sortedByDescending { scoreMatch(original, it.first) }

        loadBestTaggedCandidate(cacheKey, sorted)
    }

    /**
     * Load the best candidate from a list of [(Track, sourceExtension)] pairs.
     * Used by the single-extension path.
     */
    private suspend fun loadBestTaggedCandidate(
        cacheKey: String,
        tagged: List<Pair<Track, Extension<*>>>,
    ): MediaState.Loaded<Track> {
        var lastError: Throwable? = null
        for ((candidate, sourceExt) in tagged.take(3)) {
            val loaded = runCatching {
                Cached.loadMedia(
                    app,
                    sourceExt,
                    MediaState.Unloaded(sourceExt.id, candidate)
                ).getOrThrow()
            }
            if (loaded.isSuccess) {
                val resolved = loaded.getOrThrow()
                if (!hasSpotifyRefs(resolved.item)) {
                    resolvedCache[cacheKey] = candidate to sourceExt.id
                    return resolved
                }
                lastError = Exception("Resolved candidate still points to spotify ids")
                continue
            }
            lastError = loaded.exceptionOrNull()
        }
        throw (lastError ?: Exception("No playable match found across playback extensions"))
    }

    private fun hasSpotifyRefs(track: Track): Boolean {
        if (track.id.startsWith("spotify:", ignoreCase = true)) return true
        if (track.extras.values.any { it.startsWith("spotify:", ignoreCase = true) }) return true
        return track.servers.any { stream ->
            stream.id.startsWith("spotify:", ignoreCase = true) ||
                    stream.extras.values.any { it.startsWith("spotify:", ignoreCase = true) }
        }
    }

    /**
     * Hard pre-filter applied before scoring. Returns false for candidates that are
     * clearly wrong, so they never reach the scorer or the network loader.
     *
     * Rejects when ANY of the following is true:
     *  1. Both sides have a known duration and they differ by more than 90 seconds.
     *  2. The original has known artists AND the candidate shares zero word-tokens
     *     with ANY of those artist names (catches tribute/karaoke channels by name).
     */
    private fun shouldConsider(original: Track, candidate: Track): Boolean {
        // 1. Duration hard filter
        val aDur = original.duration
        val bDur = candidate.duration
        if (aDur != null && bDur != null && aDur > 0 && bDur > 0) {
            if (abs(aDur - bDur) > 90_000L) return false
        }

        // 2. Artist word-token hard filter
        if (original.artists.isNotEmpty() && candidate.artists.isNotEmpty()) {
            val origHasCjk = original.artists.any { hasCjk(it.name) }
            val candHasCjk = candidate.artists.any { hasCjk(it.name) }

            val originalWords = original.artists
                .flatMap {
                    Normalizer.normalize(it.name, Normalizer.Form.NFKC)
                        .lowercase().split(NORMALIZE_NON_ALNUM)
                }
                .filter { it.length > 1 }
                .toSet()

            if (originalWords.isNotEmpty()) {
                val hasOverlap = candidate.artists.any { artist ->
                    Normalizer.normalize(artist.name, Normalizer.Form.NFKC)
                        .lowercase().split(NORMALIZE_NON_ALNUM)
                        .filter { it.length > 1 }
                        .any { it in originalWords }
                }
                if (!hasOverlap) {
                    // Fallback 1: substring containment of cleaned names.
                    // Handles cases like "YOASOBI" vs "YOASOBI (幾田りら)".
                    val origNames = original.artists.map { it.name.cleanTitle() }
                    val candNames = candidate.artists.map { it.name.cleanTitle() }
                    val hasSubstring = origNames.any { o ->
                        candNames.any { c ->
                            o.isNotBlank() && c.isNotBlank() &&
                                    (o.contains(c) || c.contains(o))
                        }
                    }
                    if (!hasSubstring) {
                        // Fallback 2: for same-script CJK, do character-level
                        // overlap. CJK artist names often share kanji/hanzi even
                        // with slight formatting differences.
                        if (origHasCjk && candHasCjk) {
                            val origChars = original.artists
                                .flatMap { it.name.filter { c -> c.isLetterOrDigit() }.toList() }
                                .toSet()
                            val candChars = candidate.artists
                                .flatMap { it.name.filter { c -> c.isLetterOrDigit() }.toList() }
                                .toSet()
                            val charOverlap = origChars.intersect(candChars).size
                            val charUnion = origChars.union(candChars).size
                            // Require >50% character overlap to pass
                            if (charUnion > 0 && charOverlap * 100 / charUnion < 50) return false
                        } else if (origHasCjk != candHasCjk) {
                            // Different scripts — allow through for scoring.
                        } else {
                            // Same script, non-CJK, no overlap at all.
                            return false
                        }
                    }
                }
            }
        }

        return true
    }

    private suspend fun resolveTrackBySearch(
        extension: Extension<*>,
        original: Track,
    ): Result<List<Track>> = runCatching {
        val artist = original.artists.firstOrNull()?.name.orEmpty().trim()

        // Strip upload/marketing noise from the original title before querying so that
        // search engines receive the cleanest possible intent (e.g. no "(Official Audio)").
        val cleanedQueryTitle = original.title.let { raw ->
            var s = Normalizer.normalize(raw, Normalizer.Form.NFKC)
            for (pattern in TITLE_NOISE) s = s.replace(pattern, " ")
            s.replace(NORMALIZE_WHITESPACE, " ").trim()
        }

        // Primary query: "cleanTitle artist" — works well on most platforms.
        val primaryQuery = if (artist.isBlank()) cleanedQueryTitle
        else "$cleanedQueryTitle $artist"

        var candidates = loadSearchCandidates(extension, primaryQuery)

        // First fallback: "artist - cleanTitle" ordering (some platforms rank better this way).
        if (candidates.none { shouldConsider(original, it) } && artist.isNotBlank()) {
            candidates = loadSearchCandidates(extension, "$artist - $cleanedQueryTitle")
        }

        // Second fallback: cleaned title only (broadest net).
        if (candidates.none { shouldConsider(original, it) } && artist.isNotBlank()) {
            candidates = loadSearchCandidates(extension, cleanedQueryTitle)
        }

        if (candidates.isEmpty()) throw Exception("No matching track found for \"$primaryQuery\"")

        // Hard-filter then de-duplicate before scoring.
        val unique = candidates
            .filter { !hasSpotifyRefs(it) && shouldConsider(original, it) }
            .distinctBy { it.id }

        if (unique.isEmpty()) throw Exception("No passing candidates for \"$primaryQuery\"")

        // Fast path: if ISRC is available, check for a definitive match first.
        if (!original.isrc.isNullOrBlank()) {
            val isrcHit = unique.firstOrNull { it.isrc.equals(original.isrc, true) }
            if (isrcHit != null) return@runCatching listOf(isrcHit) +
                    unique.filter { it.id != isrcHit.id }
                        .sortedByDescending { scoreMatch(original, it) }
        }

        unique.sortedByDescending { candidate -> scoreMatch(original, candidate) }
    }

    /**
     * Returns true when [candidate] is a confident exact match for [original].
     *
     * Criteria (any one is sufficient):
     *  - ISRC codes are non-blank and equal (case-insensitive)
     *  - Clean titles match AND primary artist names match AND duration differs by ≤ 2 000 ms
     */
    private fun isExactMatch(original: Track, candidate: Track): Boolean {
        if (hasSpotifyRefs(candidate)) return false
        // ISRC is a definitive recording identifier — sufficient on its own.
        if (!original.isrc.isNullOrBlank() && original.isrc.equals(candidate.isrc, ignoreCase = true)) {
            return true
        }
        // All three must match: clean title + primary artist + duration within 2 s.
        // If any piece of data is missing we do NOT fast-path — fall through to scoring instead.
        val titleMatch = original.title.cleanTitle() == candidate.title.cleanTitle()
        if (!titleMatch) return false
        val aPrimary = original.artists.firstOrNull()?.name?.cleanTitle()
        val bPrimary = candidate.artists.firstOrNull()?.name?.cleanTitle()
        if (aPrimary.isNullOrBlank() || bPrimary.isNullOrBlank() || aPrimary != bPrimary) return false
        val aDur = original.duration
        val bDur = candidate.duration
        if (aDur == null || bDur == null || aDur <= 0 || bDur <= 0) return false
        return abs(aDur - bDur) <= 2_000L
    }

    private suspend fun loadSearchCandidates(
        extension: Extension<*>,
        query: String,
    ): List<Track> {
        val feed = extension.getAs<SearchFeedClient, Feed<Shelf>> {
            loadSearchFeed(query)
        }.getOrThrow()

        // Prefer "Songs" tab to get only song-type results (skips videos, albums, artists).
        val songsTab = feed.tabs.firstOrNull {
            it.title.equals("Songs", ignoreCase = true) ||
                    it.title.equals("Song", ignoreCase = true)
        }

        val songTracks = extractTracks(feed.getPagedData(songsTab))

        // Also pull from the "Videos" tab — on YT Music many official CJK tracks
        // are only available as videos, so we must include them for the scorer.
        val videosTab = feed.tabs.firstOrNull {
            it.title.equals("Videos", ignoreCase = true) ||
                    it.title.equals("Video", ignoreCase = true)
        }
        val videoTracks = if (videosTab != null) extractTracks(feed.getPagedData(videosTab))
        else emptyList()

        val merged = (songTracks + videoTracks).distinctBy { it.id }
        if (merged.isNotEmpty()) return merged

        // If both tabs returned nothing, fall back to the default tab.
        return extractTracks(feed.getPagedData(null))
    }

    private suspend fun extractTracks(data: Feed.Data<Shelf>): List<Track> {
        return data.pagedData.loadPage(null).data.flatMap { shelf ->
            when (shelf) {
                is Shelf.Category -> emptyList()
                is Shelf.Item -> listOfNotNull(shelf.media as? Track)
                is Shelf.Lists.Categories -> emptyList()
                is Shelf.Lists.Items -> shelf.list.filterIsInstance<Track>()
                is Shelf.Lists.Tracks -> shelf.list
            }
        }
    }

    private fun scoreMatch(a: Track, b: Track): Int {
        var score = 0

        // ── Hard reject: candidates still carrying Spotify references ──
        if (b.id.startsWith("spotify:", ignoreCase = true)) return -500
        if (b.servers.any { s ->
                s.extras.values.any { it.startsWith("spotify:", ignoreCase = true) }
            }) return -500

        // ── Altered-version penalty ──────────────────────────────────────────
        // If the ORIGINAL is not an altered version itself (instrumental, karaoke,
        // cover, remix, etc.) but the CANDIDATE carries such a marker in its raw
        // title, subtitle, or description, penalise heavily so the real recording
        // is strongly preferred.
        val originalIsAltered = a.title.isAlteredVersion()
        val candidateIsAltered = b.title.isAlteredVersion() ||
                b.subtitle.orEmpty().isAlteredVersion() ||
                b.description.orEmpty().isAlteredVersion()
        if (!originalIsAltered && candidateIsAltered) {
            score -= 120
        }
        // Small bonus when both agree (original is e.g. an official remix and we
        // found the right remix)
        if (originalIsAltered && candidateIsAltered) {
            score += 10
        }

        // ── ISRC: definitive recording identifier (+300) ──
        val isrcMatch = !a.isrc.isNullOrBlank() && a.isrc.equals(b.isrc, true)
        if (isrcMatch) score += 300

        // ── Title (up to +80) ──
        val aTitle = a.title.cleanTitle()
        val bTitle = b.title.cleanTitle()
        if (aTitle == bTitle) {
            score += 80
        } else if (aTitle.isNotBlank() && bTitle.isNotBlank()) {
            // For CJK text, word-splitting by spaces is unreliable (no word boundaries).
            // Use character-level Jaccard for CJK, word-level for Latin.
            val aCjk = hasCjk(aTitle)
            val bCjk = hasCjk(bTitle)
            if (aCjk || bCjk) {
                // Character-level comparison for CJK.
                // Extract only CJK/letter characters for overlap, ignore spaces/punct.
                val aChars = aTitle.filter { it.isLetterOrDigit() }.toSet()
                val bChars = bTitle.filter { it.isLetterOrDigit() }.toSet()
                if (aChars.isNotEmpty() && bChars.isNotEmpty()) {
                    val overlap = aChars.intersect(bChars).size
                    val union = aChars.union(bChars).size
                    val charJaccard = overlap * 70 / union
                    score += charJaccard
                    // Bonus: one title fully contains the other (very common for
                    // CJK where the same song title may have extra qualifiers).
                    val shorter = if (aTitle.length <= bTitle.length) aTitle else bTitle
                    val longer = if (aTitle.length <= bTitle.length) bTitle else aTitle
                    if (longer.contains(shorter)) score += 15
                }
            } else {
                // Word-level Jaccard for Latin/space-delimited text.
                val aWords = aTitle.split(" ").filter { it.isNotBlank() }.toSet()
                val bWords = bTitle.split(" ").filter { it.isNotBlank() }.toSet()
                if (aWords.isNotEmpty() && bWords.isNotEmpty()) {
                    val overlap = aWords.intersect(bWords).size
                    val union = aWords.union(bWords).size
                    val jaccard = overlap * 65 / union
                    score += jaccard
                    val smaller = min(aWords.size, bWords.size)
                    if (smaller > 0 && overlap == smaller) score += 15
                }
            }
        }

        // ── Duration (up to +60, down to -70) ──
        val aDuration = a.duration
        val bDuration = b.duration
        if (aDuration != null && bDuration != null && aDuration > 0 && bDuration > 0) {
            val diff = abs(aDuration - bDuration)
            score += when {
                diff <= 1_000L  -> 60   // near-identical
                diff <= 2_000L  -> 50
                diff <= 3_000L  -> 40
                diff <= 5_000L  -> 25
                diff <= 10_000L -> 5
                diff <= 15_000L -> -25  // suspicious gap (tightened from 20 s)
                diff <= 30_000L -> -50  // likely abridged / extended edit
                else            -> -70  // clearly wrong track
            }
        }

        // ── Artist matching ──
        val aPrimary = a.artists.firstOrNull()?.name?.cleanTitle()
        val bPrimary = b.artists.firstOrNull()?.name?.cleanTitle()
        val aArtistSet = a.artists.map { it.name.cleanTitle() }.toSet()
        val bArtistSet = b.artists.map { it.name.cleanTitle() }.toSet()
        val anyArtistOverlap = aArtistSet.intersect(bArtistSet).isNotEmpty()

        if (aPrimary != null && bPrimary != null) {
            // Detect cross-script comparisons (CJK vs Latin / romanized).
            // When scripts don't overlap, names may still refer to the same
            // artist in different character sets.
            val crossScript = hasCjk(a.artists.first().name) != hasCjk(b.artists.first().name)
            when {
                aPrimary == bPrimary ->
                    score += 80  // exact primary match
                aPrimary.contains(bPrimary) || bPrimary.contains(aPrimary) ->
                    score += 50  // one name contains the other (e.g. "The Weeknd" vs "Weeknd")
                anyArtistOverlap ->
                    score += 25  // at least one featured/collaborating artist matches
                crossScript -> {
                    // Different scripts (e.g. 米津玄師 vs Kenshi Yonezu) — try secondary
                    // matching: check if the original's raw artist names appear in the
                    // candidate's subtitle, description, or raw title (some platforms
                    // embed artist info there, e.g. "YOASOBI - 夜に駆ける").
                    val origRawNames = a.artists.map { it.name.lowercase().trim() }
                    val candText = listOfNotNull(
                        b.subtitle, b.description,
                        b.artists.joinToString(" ") { it.name }
                    ).joinToString(" ").lowercase()
                    val foundInMeta = origRawNames.any { name ->
                        name.isNotBlank() && candText.contains(name)
                    }
                    // Also check reverse: candidate raw artist names in original metadata.
                    val candRawNames = b.artists.map { it.name.lowercase().trim() }
                    val origText = listOfNotNull(
                        a.subtitle, a.description,
                        a.artists.joinToString(" ") { it.name }
                    ).joinToString(" ").lowercase()
                    val foundReverse = candRawNames.any { name ->
                        name.isNotBlank() && origText.contains(name)
                    }
                    score += when {
                        foundInMeta || foundReverse -> 30  // secondary evidence of same artist
                        else -> -25  // no evidence — likely different artist
                    }
                }
                else ->
                    // No artist overlap at all — likely a cover or tribute by a different artist.
                    score -= 100
            }
        }

        // ── Additional artist overlap bonus (+20 each) ──
        if (a.artists.size > 1 || b.artists.size > 1) {
            score += (aArtistSet.intersect(bArtistSet).size * 20)
        }

        // ── Title word-count bloat penalty (up to -25) ──
        // If the candidate's cleaned title has 3+ more words than the original, it likely
        // carries qualifiers that survived noise-stripping (e.g. "Live Acoustic Session").
        val aWordCount = aTitle.split(" ").count { it.isNotBlank() }
        val bWordCount = bTitle.split(" ").count { it.isNotBlank() }
        val excessWords = bWordCount - aWordCount
        if (excessWords >= 3) score -= ((excessWords - 2) * 7).coerceAtMost(25)

        // ── Album track-order number (+10) ──
        // Rare but high-confidence: same track number in the same catalogue position.
        val aOrder = a.albumOrderNumber
        val bOrder = b.albumOrderNumber
        if (aOrder != null && bOrder != null && aOrder == bOrder) score += 10

        // ── Release year signal (+15 / +8 / -20) ──
        val aYear = a.releaseDate?.year
        val bYear = b.releaseDate?.year
        if (aYear != null && bYear != null) {
            val yearDiff = abs(aYear - bYear)
            score += when {
                yearDiff == 0  -> 15   // same release year
                yearDiff == 1  -> 8    // adjacent — may be a Jan/Dec boundary
                yearDiff <= 2  -> 0    // tolerable
                else           -> -20  // clearly a different era recording
            }
        }

        // ── Explicit flag agreement (+5 / -5) ──
        if (a.isExplicit == b.isExplicit) score += 5 else score -= 5

        // ── Play count popularity signal (up to +40) ──
        // Original recordings almost always have dramatically more plays than
        // covers/re-uploads. When multiple candidates have matching titles, this
        // is often the decisive tiebreaker, especially for CJK songs where many
        // covers exist with clean (unmarked) titles.
        val bPlays = b.plays
        if (bPlays != null && bPlays > 0) {
            score += when {
                bPlays >= 10_000_000L -> 40   // 10M+  — almost certainly official
                bPlays >= 1_000_000L  -> 30   // 1M+   — very likely official
                bPlays >= 100_000L    -> 15   // 100K+ — popular, could be either
                bPlays >= 10_000L     -> 5    // 10K+  — modest
                else                  -> 0
            }
        }

        // ── Album presence signal ──
        // When the original track belongs to an album, candidates that also have
        // album info are more likely to be official releases. Covers/re-uploads
        // rarely have album metadata.
        if (a.album != null) {
            val aAlbum = a.album?.title?.cleanTitle()
            val bAlbum = b.album?.title?.cleanTitle()
            if (!bAlbum.isNullOrBlank()) {
                score += if (!aAlbum.isNullOrBlank() && aAlbum == bAlbum) 25 else 10
            } else {
                score -= 10  // candidate has no album but original does
            }
        }

        // ── Type preference: Song > VideoSong > rest ──
        score += when (b.type) {
            Track.Type.Song -> 25
            Track.Type.VideoSong -> 10
            Track.Type.HorizontalVideo -> 0
            Track.Type.Video -> -5
            Track.Type.Podcast -> -40
        }
        return score
    }

    companion object {
        // \p{L} = any Unicode letter (Latin, Hiragana, Katakana, Kanji, Arabic, Korean, etc.)
        // Using this instead of [a-z0-9] so non-Latin titles are NOT stripped to empty string.
        private val NORMALIZE_NON_ALNUM = "[^\\p{L}0-9 ]".toRegex()
        private val NORMALIZE_WHITESPACE = "\\s+".toRegex()
        // Removes spaces that appear between CJK characters. Different sources
        // may or may not insert spaces in CJK strings; normalising them away
        // improves exact-match hit rate.
        private val NORMALIZE_CJK_SPACE = "(?<=[\u3040-\u9FFF\uAC00-\uD7AF])\\s+(?=[\u3040-\u9FFF\uAC00-\uD7AF])".toRegex()

        /**
         * Returns true if [s] contains CJK characters (Chinese, Japanese Kanji,
         * Hiragana, Katakana, or Korean Hangul).
         */
        private fun hasCjk(s: String): Boolean = s.any {
            it.code in 0x4E00..0x9FFF ||  // CJK Unified Ideographs
                    it.code in 0x3040..0x309F ||  // Hiragana
                    it.code in 0x30A0..0x30FF ||  // Katakana
                    it.code in 0xAC00..0xD7AF ||  // Hangul Syllables
                    it.code in 0x3400..0x4DBF ||  // CJK Extension A
                    it.code in 0xFF65..0xFF9F     // Half-width Katakana
        }

        // Noise patterns stripped before comparison (case-insensitive).
        private val TITLE_NOISE = listOf(
            "\\(official(\\s+(music|lyric|audio|hd|hq))?\\s*video\\)",
            "\\[official(\\s+(music|lyric|audio|hd|hq))?\\s*video]",
            "\\(official\\s+audio\\)",
            "\\[official\\s+audio]",
            "\\(official\\)",
            "\\[official]",
            "\\(lyrics?\\)",
            "\\[lyrics?]",
            "\\(audio\\)",
            "\\[audio]",
            "\\(visuali[sz]er\\)",
            "\\[visuali[sz]er]",
            "\\(live\\)",
            "\\[live]",
            "\\(hd\\)",
            "\\[hd]",
            "\\(hq\\)",
            "\\[hq]",
            "\\(4k\\)",
            "\\[4k]",
            "\\(feat\\.?\\s+[^)]+\\)",
            "\\[feat\\.?\\s+[^]]+]",
            "\\(ft\\.?\\s+[^)]+\\)",
            "\\[ft\\.?\\s+[^]]+]",
            "\\(with\\s+[^)]+\\)",
            "-\\s*remaster(ed)?\\s*(\\d{4})?",
            "\\(remaster(ed)?\\s*(\\d{4})?\\)",
            "\\[remaster(ed)?\\s*(\\d{4})?]",
            "\\(\\d{4}\\s+remaster(ed)?\\)",
            "\\[\\d{4}\\s+remaster(ed)?]",
            // Year-only suffix/brackets that add zero meaning
            "\\(\\d{4}\\)",
            "\\[\\d{4}]",
            // Upload / streaming noise
            "\\(slowed(\\s+(reverb|\\+\\s*reverb|and\\s+reverb))?\\)",
            "\\[slowed(\\s+(reverb|\\+\\s*reverb|and\\s+reverb))?]",
            "\\(sped[- ]?up\\)",
            "\\[sped[- ]?up]",
            "\\(speed\\s*up\\)",
            "\\[speed\\s*up]",
            "\\(nightcore\\)",
            "\\[nightcore]",
            // CJK title noise
            "\\(公式\\)",              // Japanese: "(official)"
            "\\[公式]",
            "\\(完整版\\)",            // Chinese: "(full version)"
            "\\[完整版]",
            "\\(官方\\)",              // Chinese: "(official)"
            "\\[官方]",
            "\\(官方MV\\)",            // Chinese: "(official MV)"
            "\\[官方MV]",
            "\\(高音質\\)",            // Japanese: "(high quality)"
            "\\[高音質]",
            "\\(MV\\)",
            "\\[MV]",
            "【[^】]*】",               // Japanese/Chinese bracket markers (e.g. 【公式MV】)
            "「[^」]*」"                // Japanese quote markers often used for tags
        ).map { it.toRegex(RegexOption.IGNORE_CASE) }

        /**
         * Patterns whose presence in a candidate's *raw* title indicates an altered version
         * (instrumental, karaoke, cover, tribute, etc.).
         * If the ORIGINAL track's raw title does NOT also contain one of these markers,
         * we apply a heavy penalty so the original recording is strongly preferred.
         */
        private val ALTERED_VERSION_PATTERNS = listOf(
            "\\binstrumental\\b",
            "\\bkaraoke\\b",
            "\\bcover\\b",
            "\\btribute\\b",
            "\\bremake\\b",
            "\\bacoustic\\b",
            "\\bpiano\\s+version\\b",
            "\\borchestral\\b",
            "\\bno\\s+vocals?\\b",
            "\\bwithout\\s+vocals?\\b",
            "\\bvocal\\s+remov",
            "\\bslowed\\b",
            "\\bnightcore\\b",
            "\\bsped[- ]?up\\b",
            "\\bspeed\\s*up\\b",
            "\\bremix\\b",
            "\\bedit\\b",
            "\\bvip\\s+(mix|edit)\\b",
            "\\bextended\\s+(mix|version)\\b",
            "\\bradio\\s+edit\\b",
            "\\bclimax\\s+mix\\b",
            "\\bclub\\s+(mix|edit|version)\\b",
            "\\bdub\\s+(mix|version)\\b",
            "\\bbacking\\s+track\\b",
            "\\bfan\\s+(made|edit)\\b",
            "\\bun?official\b",
            // CJK cover/non-original markers
            "歌ってみた",     // Japanese: "tried singing" (common cover tag)
            "弾いてみた",     // Japanese: "tried playing" (instrumental cover)
            "演奏してみた",   // Japanese: "tried performing"
            "叩いてみた",     // Japanese: "tried drumming"
            "カバー",         // Japanese katakana for "cover"
            "翻唱",           // Chinese: "cover"
            "伴奏",           // Chinese: "accompaniment" / instrumental
            "cover\\b"       // common in CJK titles mixed with English
        ).map { it.toRegex(RegexOption.IGNORE_CASE) }

        /** LRU cache: "primaryExtId:originalTrackId" → (resolved candidate Track, source extension ID) */
        private val resolvedCache =
            object : LinkedHashMap<String, Pair<Track, String>>(64, 0.75f, true) {
                override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Pair<Track, String>>?) =
                    size > 200
            }
    }

    /** Normalize and strip noise from a title / artist name for comparison. */
    private fun String.cleanTitle(): String {
        // NFKC normalization converts full-width characters to half-width equivalents
        // (e.g. Ａ→A, ０→0, half-width katakana→full-width) so CJK and Latin text
        // can be compared reliably.
        var s = Normalizer.normalize(this, Normalizer.Form.NFKC)
        for (pattern in TITLE_NOISE) s = s.replace(pattern, " ")
        return s.lowercase()
            .replace(NORMALIZE_NON_ALNUM, " ")
            .replace(NORMALIZE_CJK_SPACE, "")  // remove spaces between CJK runs
            .replace(NORMALIZE_WHITESPACE, " ")
            .trim()
    }

    /** Returns true if the raw title contains an altered-version marker (instrumental, karaoke, cover, etc.). */
    private fun String.isAlteredVersion(): Boolean =
        ALTERED_VERSION_PATTERNS.any { it.containsMatchIn(this) }

    private suspend fun loadServer(mediaItem: MediaItem): Result<Streamable.Media.Server> {
        val downloaded = mediaItem.downloaded
        val servers = mediaItem.track.servers
        val index = mediaItem.serverIndex
        if (!downloaded.isNullOrEmpty() && servers.size == index) {
            return runCatching {
                Streamable.Media.Server(
                    downloaded.map { Uri.fromFile(File(it)).toString().toSource() },
                    true
                )
            }
        }
        return withClient(mediaItem) {
            runCatching {
                val isPlayable = mediaItem.track.playableString(app.context)
                if (isPlayable != null) throw Exception(isPlayable)
                val streamable = servers.getOrNull(index) ?: throw Exception("Server not found")
                loadStreamableMedia(
                    app, it, mediaItem.track, streamable
                ).getOrThrow() as Streamable.Media.Server
            }
        }
    }

    private suspend fun loadBackground(mediaItem: MediaItem): Result<Streamable.Media.Background> {
        val streams = mediaItem.track.backgrounds
        val index = mediaItem.backgroundIndex
        val streamable = streams[index]
        return withClient(mediaItem) {
            runCatching {
                loadStreamableMedia(
                    app, it, mediaItem.track, streamable
                ).getOrThrow() as Streamable.Media.Background
            }
        }
    }

    private suspend fun loadSubtitle(mediaItem: MediaItem): Result<Streamable.Media.Subtitle> {
        val streams = mediaItem.track.subtitles
        val index = mediaItem.subtitleIndex
        val streamable = streams[index]
        return withClient(mediaItem) {
            runCatching {
                loadStreamableMedia(
                    app, it, mediaItem.track, streamable
                ).getOrThrow() as Streamable.Media.Subtitle
            }
        }
    }
}
