package dev.brahmkshatriya.echo.ui.stats

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dev.brahmkshatriya.echo.analytics.db.PlayHistoryDao
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

class StatsViewModel(
    private val dao: PlayHistoryDao
) : ViewModel() {

    enum class TimeFilter(val days: Int?) {
        WEEK(7), MONTH(30), ALL(null)
    }

    data class Overview(
        val totalListeningTimeMs: Long = 0,
        val totalPlays: Int = 0,
    )

    private val _timeFilter = MutableStateFlow(TimeFilter.ALL)
    val timeFilter = _timeFilter.asStateFlow()

    private val _overview = MutableStateFlow(Overview())
    val overview = _overview.asStateFlow()

    private val _tracks = MutableStateFlow<List<StatEntry>>(emptyList())
    val tracks = _tracks.asStateFlow()

    private val _isLoading = MutableStateFlow(false)
    val isLoading = _isLoading.asStateFlow()

    private var currentPage = 0
    private var hasMore = true
    private var isLoadingMore = false

    init {
        loadInitial()
    }

    fun setTimeFilter(filter: TimeFilter) {
        _timeFilter.value = filter
        loadInitial()
    }

    private fun sinceTimestamp(): Long {
        return _timeFilter.value.days?.let {
            System.currentTimeMillis() - it * 24 * 60 * 60 * 1000L
        } ?: 0L
    }

    private fun loadInitial() {
        viewModelScope.launch {
            _isLoading.value = true
            currentPage = 0
            hasMore = true

            val since = sinceTimestamp()
            _overview.value = Overview(
                totalListeningTimeMs = dao.getTotalListeningTime(since),
                totalPlays = dao.getTotalPlays(since),
            )

            val tracks = dao.getTopTracks(since, PAGE_SIZE, 0)
            hasMore = tracks.size >= PAGE_SIZE
            currentPage = 1
            _tracks.value = tracks.mapIndexed { i, it ->
                StatEntry(i + 1, it.trackTitle, it.artistNames, it.playCount, it.coverUrl, it.trackId, it.extensionId)
            }
            _isLoading.value = false
        }
    }

    fun loadMore() {
        if (!hasMore || isLoadingMore) return
        isLoadingMore = true
        viewModelScope.launch {
            val since = sinceTimestamp()
            val offset = currentPage * PAGE_SIZE
            val newTracks = dao.getTopTracks(since, PAGE_SIZE, offset)
            hasMore = newTracks.size >= PAGE_SIZE
            currentPage++

            val currentList = _tracks.value
            val startRank = currentList.size + 1
            _tracks.value = currentList + newTracks.mapIndexed { i, it ->
                StatEntry(startRank + i, it.trackTitle, it.artistNames, it.playCount, it.coverUrl, it.trackId, it.extensionId)
            }
            isLoadingMore = false
        }
    }

    companion object {
        private const val PAGE_SIZE = 20
    }
}
