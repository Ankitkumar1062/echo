package dev.brahmkshatriya.echo.ui.stats

import android.os.Bundle
import android.view.View
import androidx.core.view.isVisible
import androidx.core.view.updatePadding
import androidx.fragment.app.Fragment
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.android.material.transition.MaterialSharedAxis
import dev.brahmkshatriya.echo.R
import dev.brahmkshatriya.echo.common.models.ImageHolder.Companion.toImageHolder
import dev.brahmkshatriya.echo.common.models.Track
import dev.brahmkshatriya.echo.databinding.FragmentStatsBinding
import dev.brahmkshatriya.echo.ui.common.UiViewModel
import dev.brahmkshatriya.echo.ui.player.PlayerViewModel
import dev.brahmkshatriya.echo.ui.common.UiViewModel.Companion.applyBackPressCallback
import dev.brahmkshatriya.echo.ui.common.UiViewModel.Companion.applyInsets
import dev.brahmkshatriya.echo.ui.common.UiViewModel.Companion.configure
import dev.brahmkshatriya.echo.utils.ContextUtils.observe
import dev.brahmkshatriya.echo.utils.ui.AnimationUtils.setupTransition
import kotlinx.coroutines.flow.combine
import org.koin.androidx.viewmodel.ext.android.activityViewModel
import org.koin.androidx.viewmodel.ext.android.viewModel
import java.text.NumberFormat

class StatsFragment : Fragment(R.layout.fragment_stats) {

    private val vm by viewModel<StatsViewModel>()

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        val binding = FragmentStatsBinding.bind(view)
        setupTransition(view, false, MaterialSharedAxis.Y)

        val uiViewModel by activityViewModel<UiViewModel>()
        observe(uiViewModel.navigationReselected) {
            if (it != 3) return@observe
            vm.setTimeFilter(vm.timeFilter.value)
        }
        observe(uiViewModel.navigation.combine(uiViewModel.mainBgDrawable) { a, b -> a to b }) {
            (curr, _) ->
            if (curr != 3) return@observe
            uiViewModel.currentNavBackground.value = null
        }

        applyInsets {
            binding.statsContainer.updatePadding(top = it.top)
            binding.tracksRecycler.updatePadding(bottom = it.bottom)
            binding.appBarOutline.updatePadding(top = it.top)
            binding.swipeRefresh.configure(it)
        }
        applyBackPressCallback()

        binding.swipeRefresh.setOnRefreshListener {
            vm.setTimeFilter(vm.timeFilter.value)
        }

        binding.chipGroup.setOnCheckedStateChangeListener { _, checkedIds ->
            val filter = when (checkedIds.firstOrNull()) {
                R.id.chipWeek -> StatsViewModel.TimeFilter.WEEK
                R.id.chipMonth -> StatsViewModel.TimeFilter.MONTH
                else -> StatsViewModel.TimeFilter.ALL
            }
            vm.setTimeFilter(filter)
        }

        val nf = NumberFormat.getNumberInstance()
        val playerVm by activityViewModel<PlayerViewModel>()
        val trackAdapter = StatEntryAdapter { entry ->
            val trackId = entry.trackId ?: return@StatEntryAdapter
            val extensionId = entry.extensionId ?: return@StatEntryAdapter
            val track = Track(
                id = trackId,
                title = entry.title,
                cover = entry.coverUrl?.toImageHolder(),
            )
            playerVm.play(extensionId, track, false)
        }
        binding.tracksRecycler.adapter = trackAdapter

        // Infinite scroll
        binding.tracksRecycler.addOnScrollListener(object : RecyclerView.OnScrollListener() {
            override fun onScrolled(recyclerView: RecyclerView, dx: Int, dy: Int) {
                val lm = recyclerView.layoutManager as LinearLayoutManager
                val totalItemCount = lm.itemCount
                val lastVisible = lm.findLastVisibleItemPosition()
                if (totalItemCount - lastVisible <= 5) {
                    vm.loadMore()
                }
            }
        })

        observe(vm.isLoading) { binding.swipeRefresh.isRefreshing = it }

        observe(vm.overview) { overview ->
            val totalMinutes = overview.totalListeningTimeMs / 60_000
            binding.totalTime.text = when {
                totalMinutes >= 60 -> {
                    val hours = totalMinutes / 60
                    val mins = totalMinutes % 60
                    getString(R.string.stats_time_hours_mins, nf.format(hours), nf.format(mins))
                }
                else -> getString(R.string.stats_time_mins, nf.format(totalMinutes))
            }
            binding.totalPlays.text = nf.format(overview.totalPlays)
        }

        observe(vm.tracks) { tracks ->
            trackAdapter.submitList(tracks)
            binding.topTracksEmpty.isVisible = tracks.isEmpty()
            binding.tracksRecycler.isVisible = tracks.isNotEmpty()
        }

        observe(vm.timeFilter) { filter ->
            val chipId = when (filter) {
                StatsViewModel.TimeFilter.WEEK -> R.id.chipWeek
                StatsViewModel.TimeFilter.MONTH -> R.id.chipMonth
                StatsViewModel.TimeFilter.ALL -> R.id.chipAll
            }
            if (binding.chipGroup.checkedChipId != chipId) binding.chipGroup.check(chipId)
        }
    }
}
