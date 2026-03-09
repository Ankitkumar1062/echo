package dev.brahmkshatriya.echo.playback.source

import android.content.Context
import android.net.Uri
import androidx.annotation.OptIn
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.DataSpec
import androidx.media3.datasource.ResolvingDataSource.Resolver
import dev.brahmkshatriya.echo.common.models.Streamable
import dev.brahmkshatriya.echo.playback.MediaItemUtils.toKey
import dev.brahmkshatriya.echo.playback.source.StreamableDataSource.Companion.uri
import dev.brahmkshatriya.echo.utils.CacheUtils.saveToCache

class StreamableResolver(
    private val context: Context,
    private val current: MutableMap<String, Result<Streamable.Media.Server>>,
) : Resolver {

    @OptIn(UnstableApi::class)
    override fun resolveDataSpec(dataSpec: DataSpec): DataSpec {
        val (id, index) = dataSpec.uri.toString().toKey().getOrNull() ?: return dataSpec
        val server = current[id]?.getOrNull()
            ?: return dataSpec.copy(
                customData = Result.failure<Streamable.Source>(
                    Exception("Stream server not resolved for id=$id")
                )
            )
        val streamable: Result<Streamable.Source> =
            server.sources.getOrNull(index)?.let { Result.success<Streamable.Source>(it) }
                ?: Result.failure<Streamable.Source>(
                    Exception("Invalid source index=$index for id=$id")
                )
        val uri = streamable.map {
            if (!it.isLive)
                context.saveToCache(it.uri.toString(), dataSpec.uri.toString(), "player")
            it.uri
        }
        return dataSpec.copy(uri = uri.getOrNull(), customData = streamable)
    }

    companion object {

        @OptIn(UnstableApi::class)
        fun DataSpec.copy(
            uri: Uri? = null,
            uriPositionOffset: Long? = null,
            httpMethod: Int? = null,
            httpBody: ByteArray? = null,
            httpRequestHeaders: Map<String, String>? = null,
            position: Long? = null,
            length: Long? = null,
            key: String? = null,
            flags: Int? = null,
            customData: Any? = null,
        ): DataSpec {
            return DataSpec.Builder()
                .setUri(uri ?: this.uri)
                .setUriPositionOffset(uriPositionOffset ?: this.uriPositionOffset)
                .setHttpMethod(httpMethod ?: this.httpMethod)
                .setHttpBody(httpBody ?: this.httpBody)
                .setHttpRequestHeaders(httpRequestHeaders ?: this.httpRequestHeaders)
                .setPosition(position ?: this.position)
                .setLength(length ?: this.length)
                .setKey(key ?: this.key)
                .setFlags(flags ?: this.flags)
                .setCustomData(customData ?: this.customData)
                .build()
        }
    }
}