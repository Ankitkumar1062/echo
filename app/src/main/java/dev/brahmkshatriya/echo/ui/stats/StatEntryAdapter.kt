package dev.brahmkshatriya.echo.ui.stats

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.DiffUtil
import androidx.recyclerview.widget.ListAdapter
import androidx.recyclerview.widget.RecyclerView
import dev.brahmkshatriya.echo.R
import dev.brahmkshatriya.echo.common.models.ImageHolder.Companion.toImageHolder
import dev.brahmkshatriya.echo.databinding.ItemStatEntryBinding
import dev.brahmkshatriya.echo.utils.image.ImageUtils.loadInto

data class StatEntry(
    val rank: Int,
    val title: String,
    val subtitle: String?,
    val playCount: Int,
    val coverUrl: String? = null,
    val trackId: String? = null,
    val extensionId: String? = null,
)

class StatEntryAdapter(
    private val onItemClick: ((StatEntry) -> Unit)? = null
) : ListAdapter<StatEntry, StatEntryAdapter.ViewHolder>(DIFF) {

    class ViewHolder(
        val binding: ItemStatEntryBinding,
    ) : RecyclerView.ViewHolder(binding.root)

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) = ViewHolder(
        ItemStatEntryBinding.inflate(LayoutInflater.from(parent.context), parent, false)
    )

    override fun onBindViewHolder(holder: ViewHolder, position: Int) {
        val item = getItem(position)
        with(holder.binding) {
            statRank.text = item.rank.toString()
            statTitle.text = item.title
            statSubtitle.text = item.subtitle ?: ""
            statSubtitle.visibility =
                if (item.subtitle.isNullOrBlank()) android.view.View.GONE else android.view.View.VISIBLE
            statCount.text = root.context.getString(R.string.stats_n_plays, item.playCount)
            item.coverUrl?.toImageHolder()
                .loadInto(statCover, R.drawable.art_music)
            root.setOnClickListener { onItemClick?.invoke(item) }
        }
    }

    companion object {
        val DIFF = object : DiffUtil.ItemCallback<StatEntry>() {
            override fun areItemsTheSame(a: StatEntry, b: StatEntry) =
                a.rank == b.rank && a.title == b.title

            override fun areContentsTheSame(a: StatEntry, b: StatEntry) = a == b
        }
    }
}
