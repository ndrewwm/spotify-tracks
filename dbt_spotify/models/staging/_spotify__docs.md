{% docs track_id %}
Unique ID for this track.
{% enddocs %}

{% docs play_id %}
Unique ID for this track play.
{% enddocs %}

{% docs track_name %}
The track's title.
{% enddocs %}

{% docs album_id %}
Unique ID for this album.
{% enddocs %}

{% docs artist_id %}
Unique ID for the track's artist(s).
{% enddocs %}

{% docs track_album %}
The track's album name.
{% enddocs %}

{% docs track_artists %}
Comma separated list of artists that perform on the track.
{% enddocs %}

{% docs played_at %}
UTC timestamp, marking when the track was played.
{% enddocs %}

{% docs played_at_mtn %}
Timestamp, localized to America/Boise, marking when the track was played.
{% enddocs %}

{% docs popularity %}
Integer value ranging from 0 to 100, with 100 indicating highest popularity. See [Spotify's Docs](https://developer.spotify.com/documentation/web-api/reference/get-recently-played) for an overview of the metric.
{% enddocs %}

{% docs album_release_date %}
Album release date. Sometimes only the *year* of the album's release is available. In this case, the value is standardized to a date of 1/1/YYYY.
{% enddocs %}

{% docs context %}
Context in which the track was played. One of: "album", "playlist", "artist", or null.
{% enddocs %}

{% docs duration_ms %}
Track's duration in milliseconds (ms).
{% enddocs %}
