// Copyright 2021 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @fileoverview Youtube API Client Library.
 */

'use strict';

const {google} = require('googleapis');
const { GoogleApiClient } = require('./base/google_api_client.js');
const {
  Schema$Channel,
  Schema$Video,
  Schema$CommentThread,
  Schema$Playlist,
  Schema$Search,
} = google.youtube;
const { getLogger } = require('../components/utils.js');

const API_SCOPES = Object.freeze([
  'https://www.googleapis.com/auth/youtube.force-ssl'
]);
const API_VERSION = 'v3';

/**
 * Configuration for listing youtube channels.
 * @see https://developers.google.com/youtube/v3/docs/channels/list
 * @typedef {{
 *   part: Array<string>,
 *   categoryId: (string|undefined),
 *   forUsername: (string|undefined),
 *   id: (string|undefined),
 *   managedByMe: (boolean|undefined),
 *   mine: (boolean|undefined),
 *   hl: (string|undefined),
 *   onBehalfOfContentOwner: (string|undefined),
 *   pageToken: (string|undefined),
 * }}
 */
let ListChannelsConfig;

/**
 * Configuration for listing youtube videos.
 * @see https://developers.google.com/youtube/v3/docs/videos/list
 * @typedef {{
 *   part: Array<string>,
 *   id: (string|undefined),
 *   chart: (string|undefined),
 *   hl: (string|undefined),
 *   maxHeight: (unsigned integer|undefined),
 *   maxResults: (unsigned integer|undefined),
 *   maxWidth: (unsigned integer|undefined),
 *   onBehalfOfContentOwner: (string|undefined),
 *   pageToken: (string|undefined),
 *   regionCode: (string|undefined),
 * }}
 */
let ListVideosConfig;

/**
 * Configuration for listing youtube comment threads by the given id.
 * @see https://developers.google.com/youtube/v3/docs/commentThreads/list
 * @typedef {{
 *   part: Array<string>,
 *   id: (string|undefined),
 *   allThreadsRelatedToChannelId:	(string|undefined),
 *   channelId:	(string|undefined),
 *   videoId:	(string|undefined),
 *   maxResults: (unsigned integer|undefined),
 *   moderationStatus: (string|undefined),
 *   order: (string|undefined),
 *   pageToken: (string|undefined),
 *   searchTerms: (string|undefined),
 *   textFormat: (string|undefined),
 *   limit: (unsigned integer|undefined),
 * }}
 */
let ListCommentThreadsConfig;

/**
 * Configuration for listing youtube play list.
 * @see https://developers.google.com/youtube/v3/docs/Playlists/list
 * @typedef {{
 *   part: Array<string>,
 *   channelId:	(string|undefined),
 *   id: (string|undefined),
 *   mine:	(boolean|undefined),
 *   hl:	(string|undefined),
 *   maxResults: (unsigned integer|undefined),
 *   onBehalfOfContentOwner: (string|undefined),
 *   onBehalfOfContentOwnerChannel: (string|undefined),
 *   pageToken: (string|undefined)
 * }}
 */
let ListPlaylistConfig;

/**
 * Configuration for listing youtube search results.
 * @see https://developers.google.com/youtube/v3/docs/search/list
 * @typedef {{
 *   part: Array<string>,
 *   forContentOwner:	(boolean|undefined),
 *   forDeveloper:	(boolean|undefined),
 *   forMine:	(boolean|undefined),
 *   relatedToVideoId:	(string|undefined),
 *   channelId:	(string|undefined),
 *   channelType:	(string|undefined),
 *   eventType:	(string|undefined),
 *   location:	(string|undefined),
 *   locationRadius:	(string|undefined),
 *   maxResults: (unsigned integer|undefined),
 *   onBehalfOfContentOwner:	(string|undefined),
 *   order:	(string|undefined),
 *   pageToken:	(string|undefined),
 *   publishedAfter: (datetime|undefined),
 *   publishedBefore: (datetime|undefined),
 *   q:	(string|undefined),
 *   regionCode:	(string|undefined),
 *   relevanceLanguage:	(string|undefined),
 *   safeSearch:	(string|undefined),
 *   topicId:	(string|undefined),
 *   type:	(string|undefined),
 *   videoCaption:	(string|undefined),
 *   videoCategoryId:	(string|undefined),
 *   videoDefinition:	(string|undefined),
 *   videoDimension:	(string|undefined),
 *   videoDuration:	(string|undefined),
 *   videoEmbeddable:	(string|undefined),
 *   videoLicense:	(string|undefined),
 *   videoSyndicated:	(string|undefined),
 *   videoType:	(string|undefined)
 * }}
 */
let ListSearchConfig;

/**
 * Youtube API v3 stub.
 * See: https://developers.google.com/youtube/v3/docs
 * Channel list type definition, see:
 * https://developers.google.com/youtube/v3/docs/channels/list
 * Video list type definition, see:
 * https://developers.google.com/youtube/v3/docs/videos/list
 * CommentThread list type definition, see:
 * https://developers.google.com/youtube/v3/docs/commentThreads/list
 * Playlist list type definition, see:
 * https://developers.google.com/youtube/v3/docs/playlists/list
 * Search list type definition, see:
 * https://developers.google.com/youtube/v3/docs/search/list
 */
class YouTube extends GoogleApiClient {
  /**
   * @constructor
   * @param {!Object<string,string>=} env The environment object to hold env
   *     variables.
   */
  constructor(env = process.env) {
    super(env);
    this.googleApi = 'youtube';
    this.logger = getLogger('API.YT');
  }

  /** @override */
  getScope() {
    return API_SCOPES;
  }

  /** @override */
  getVersion() {
    return API_VERSION;
  }

  /**
   * Returns a collection of zero or more channel resources that match the
   * request criteria.
   * @see https://developers.google.com/youtube/v3/docs/channels/list
   * @param {!ListChannelsConfig} config List channels configuration.
   * @return {!Promise<Array<Schema$Channel>>}
   */
  async listChannels(config) {
    const channelListRequest = Object.assign({}, config);
    channelListRequest.part = channelListRequest.part.join(',')
    try {
      const youtube = await this.getApiClient();
      const response = await youtube.channels.list(channelListRequest);
      this.logger.debug('Response: ', response);
      return response.data.items;
    } catch (error) {
      const errorMsg =
          `Fail to list channels: ${JSON.stringify(channelListRequest)}`;
      this.logger.error('YouTube list channels failed.', error.message);
      this.logger.debug('Errors in response:', error);
      throw new Error(errorMsg);
    }
  }

  /**
   * Returns a list of videos that match the API request parameters.
   * @see https://developers.google.com/youtube/v3/docs/videos/list
   * @param {!ListVideosConfig} config List videos configuration.
   * @return {!Promise<Array<Schema$Video>>}
   */
  async listVideos(config) {
    const videoListRequest = Object.assign({}, config);
    videoListRequest.part = videoListRequest.part.join(',')
    try {
      const youtube = await this.getApiClient();
      const response = await youtube.videos.list(videoListRequest);
      this.logger.debug('Response: ', response);
      return response.data.items;
    } catch (error) {
      const errorMsg =
          `Fail to list videos: ${JSON.stringify(videoListRequest)}`;
      this.logger.error('YouTube list videos failed.', error.message);
      this.logger.debug('Errors in response:', error);
      throw new Error(errorMsg);
    }
  }

  /**
   * Returns a list of comment threads that match the API request parameters.
   * @see https://developers.google.com/youtube/v3/docs/videos/list
   * @param {!ListCommentThreadsConfig} config List comment threads
   *   configuration.
   * @return {!Promise<Array<Schema$CommentThread>>}
   */
  async listCommentThreads(config) {
    const commentThreadsRequest = Object.assign({}, config);
    commentThreadsRequest.part = commentThreadsRequest.part.join(',')
    try {
      const youtube = await this.getApiClient();
      const response = await youtube.commentThreads.list(commentThreadsRequest);
      this.logger.debug('Response: ', response.data);
      return response.data.items;
    } catch (error) {
      const errorMsg =
        `Fail to list comment threads: ${JSON.stringify(commentThreadsRequest)}`;
      this.logger.error('YouTube list comment threads failed.', error.message);
      this.logger.debug('Errors in response:', error);
      throw new Error(errorMsg);
    }
  }

  /**
   * Returns a collection of playlists that match the API request parameters.
   * @see https://developers.google.com/youtube/v3/docs/playlists/list
   * @param {!ListPlaylistConfig} config List playlist configuration.
   * @param {number} resultLimit The limit of the number of results.
   * @param {string} pageToken Token to identify a specific page in the result.
   * @return {!Promise<Array<Schema$Playlist>>}
   */
  async listPlaylists(config, resultLimit = 1000, pageToken = null) {
    if (resultLimit <= 0) return [];

    const playlistsRequest = Object.assign({
        // auth: this.auth,
    }, config, {
        pageToken
    });

    if (Array.isArray(playlistsRequest.part)) {
      playlistsRequest.part = playlistsRequest.part.join(',');
    }

    try {
      const youtube = await this.getApiClient();
      const response = await youtube.playlists.list(playlistsRequest);
      this.logger.debug('Response: ', response.data);
      if (response.data.nextPageToken) {
        this.logger.debug(
          'Call youtube playlist:list API agian with Token: ',
          response.data.nextPageToken);
        const nextResponse = await this.listPlaylists(
          config,
          resultLimit - response.data.items.length,
          response.data.nextPageToken);
        return response.data.items.concat(nextResponse);
      }
      return response.data.items;
    } catch (error) {
      const errorMsg =
        `Fail to list playlists: ${JSON.stringify(playlistsRequest)}`;
      this.logger.error('YouTube list playlists failed.', error.message);
      this.logger.debug('Errors in response:', error);
      throw new Error(errorMsg);
    }
  }

  /**
   * Returns a collection of search results that match the query parameters
   * specified in the API request.
   * @see https://developers.google.com/youtube/v3/docs/search/list
   * @param {!ListSearchConfig} config List search result configuration.
   * @param {number} resultLimit The limit of the number of results.
   * @param {string} pageToken Token to identify a specific page in the result.
   * @return {!Promise<Array<Schema$Search>>}
   */
  async listSearchResults(config, resultLimit = 1000, pageToken = null) {
    if (resultLimit <= 0) return [];

    const searchRequest = Object.assign({}, config, { pageToken });

    if (Array.isArray(searchRequest.part)) {
      searchRequest.part = searchRequest.part.join(',');
    }

    try {
      const youtube = await this.getApiClient();
      const response = await youtube.search.list(searchRequest);
      this.logger.debug('Response: ', response.data);
      if (response.data.nextPageToken) {
        this.logger.debug(
          'Call youtube search:list API agian with Token: ',
          response.data.nextPageToken);
        const nextResponse = await this.listSearchResults(
          config,
          resultLimit - response.data.items.length,
          response.data.nextPageToken);
        return response.data.items.concat(nextResponse);
      }
      return response.data.items;
    } catch (error) {
      const errorMsg =
        `Fail to list search results: ${JSON.stringify(searchRequest)}`;
      this.logger.error('YouTube list search results failed.', error.message);
      this.logger.debug('Errors in response:', error);
      throw new Error(errorMsg);
    }
  }
}

module.exports = {
  YouTube,
  ListChannelsConfig,
  ListVideosConfig,
  ListCommentThreadsConfig,
  ListPlaylistConfig,
  ListSearchConfig,
  API_VERSION,
  API_SCOPES,
};
