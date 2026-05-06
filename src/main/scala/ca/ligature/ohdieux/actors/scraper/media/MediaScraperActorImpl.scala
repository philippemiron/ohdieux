package ca.ligature.ohdieux.actors.scraper.media

import ca.ligature.ohdieux.ohdio.{ApiClient, RCModels}
import ca.ligature.ohdieux.persistence.{
  MediaEntity,
  MediaRepository,
  EpisodeRepository
}
import scala.collection.immutable.HashSet
import play.api.Logger

private case class MediaScraperActorImpl(
    api: ApiClient,
    mediaRepository: MediaRepository,
    episodeRepository: EpisodeRepository,
    archiveBlacklist: HashSet[Int],
    shouldRefreshMediaUrl: (
        mediaId: Int,
        currentUrl: Option[String]
    ) => Boolean,
    onNewMedia: (
        mediaId: Int,
        mediaUrl: String,
        skipDownload: Boolean,
        programmeId: Int
    ) => Unit
) {
  var logger: Logger = Logger(this.getClass())

  def fetchEpisodeMedia(
      episode: RCModels.ProgrammeContentDetailItem,
      parentProgrammeId: Int
  ): Unit = {
    for (mediaId, i) <- fetchMediaIds(episode).zipWithIndex do {
      val alreadySaved = mediaRepository.getById(mediaId.toInt)
      if (
        alreadySaved.isEmpty || shouldRefreshMediaUrl(
          mediaId.toInt,
          alreadySaved.map(_.upstream_url)
        )
      ) {
        fetchMediaUrl(mediaId.toInt) match {
          case Some(mediaUrl) =>
            onNewMedia(
              mediaId.toInt,
              mediaUrl,
              archiveBlacklist.contains(parentProgrammeId),
              parentProgrammeId
            )
            mediaRepository.save(
              MediaEntity(
                id = mediaId.toInt,
                episode_id = episode.playlistItemId.globalId2.id.toInt,
                episode_index = i,
                length = episode.duration.durationInSeconds,
                upstream_url = mediaUrl
              )
            )
          case None =>
            logger.warn(
              s"Could not fetch stream for media ${mediaId}, skipping"
            )
        }
      }
    }
  }

  def retriggerDownloads(programmeId: Int): Unit = {
    for (episode <- episodeRepository.getByProgrammeId(programmeId)) do {
      val media = mediaRepository.getByEpisodeId(episode.id)
      for (m <- media) do {
        onNewMedia(
          m.id,
          m.upstream_url,
          archiveBlacklist.contains(programmeId),
          programmeId
        )
      }
    }
  }

  private def fetchMediaIds(
      episode: RCModels.ProgrammeContentDetailItem
  ): Seq[String] =
    episode.playlistItemId.mediaId
      .map(Seq(_))
      .getOrElse(
        fetchMediaIdsFromPlaybackList(episode.playlistItemId)
      )

  private def fetchMediaIdsFromPlaybackList(
      playlistItemId: RCModels.PlaylistItemId
  ): Seq[String] = {
    val playbackList = api
      .getPlaybacklistById(
        playlistItemId.globalId2.contentType.id,
        playlistItemId.globalId2.id
      )
      .get

    playbackList.items
      .filter(
        _.mediaPlaybackItem.globalId.id == playlistItemId.globalId2.id
      )
      .map(_.mediaPlaybackItem.mediaId)
      .distinct

  }

  private def fetchMediaUrl(mediaId: Int): Option[String] = {
    val result = TECHS
      .flatMap(tech => {
        val fetchResult = api.getMedia(mediaId, tech)
        if (!fetchResult.isSuccess) {
          logger.warn(s"getMedia(${mediaId}, ${tech}) failed: ${fetchResult}")
        }
        fetchResult.opt
      })
      .map(_.url)
      .headOption
    result
  }

}

private case class MediaUrl(tech: "hls" | "progressive", url: String);

private val TECHS: LazyList["hls" | "progressive"] =
  LazyList("progressive", "hls")
