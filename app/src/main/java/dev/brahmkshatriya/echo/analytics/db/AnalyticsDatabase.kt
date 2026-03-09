package dev.brahmkshatriya.echo.analytics.db

import android.app.Application
import androidx.room.Database
import androidx.room.Room
import androidx.room.RoomDatabase

@Database(
    entities = [PlayHistoryEntity::class],
    version = 2,
    exportSchema = false
)
abstract class AnalyticsDatabase : RoomDatabase() {
    abstract fun playHistoryDao(): PlayHistoryDao

    companion object {
        private const val DATABASE_NAME = "analytics-db"
        fun create(app: Application) = Room.databaseBuilder(
            app, AnalyticsDatabase::class.java, DATABASE_NAME
        ).fallbackToDestructiveMigration(true).build()
    }
}
