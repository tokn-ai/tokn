"""Reusable text and media values. Resource resolution never performs implicit I/O."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from typing import Any, ClassVar, Literal

AudioFormat = Literal["wav", "mp3", "flac", "opus", "pcm16", "aac"]
ImageDetail = Literal["auto", "low", "high", "original"]


def _mapping(value: object, name: str) -> Mapping[str, Any]:
  if not isinstance(value, Mapping):
    raise TypeError(f"{name} must be a mapping")
  return value


def _text(value: object, name: str) -> str:
  if not isinstance(value, str):
    raise TypeError(f"{name} must be a string")
  return value


def _integer(value: object, name: str, minimum: int = 0) -> int:
  if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= 2**64 - 1:
    raise ValueError(f"{name} must be an integer between {minimum} and {2**64 - 1}")
  return value


def _optional(data: Mapping[str, Any], name: str, parser: Any) -> Any:
  value = data.get(name)
  return None if value is None else parser(value, name)


def _encode(value: Any) -> Any:
  if isinstance(value, Content):
    return value.to_dict(include_type=False)
  if isinstance(value, _Value):
    return value.to_dict()
  if isinstance(value, (tuple, list)):
    return [_encode(item) for item in value]
  return value


class _Value:
  __slots__ = ()

  def to_dict(self) -> dict[str, Any]:
    return {
      item.name: _encode(getattr(self, item.name))
      for item in fields(self)
      if getattr(self, item.name) is not None
    }


@dataclass(slots=True)
class ProviderAssetScope(_Value):
  provider: str = field(repr=False)
  account: str = field(repr=False)
  backend: str = field(repr=False)
  upstream_account_digest: str | None = field(default=None, repr=False)


class MediaSource(_Value):
  __slots__ = ()
  kind: ClassVar[str]

  def to_dict(self) -> dict[str, Any]:
    return {"type": self.kind, **_Value.to_dict(self)}

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> MediaSource:
    kind = data.get("type")
    if kind == "inline":
      return InlineSource(base64.b64decode(_text(data.get("data"), "data"), validate=True))
    if kind == "remote":
      return RemoteSource(_text(data.get("url"), "url"))
    if kind == "asset":
      return AssetReference(_text(data.get("id"), "id"), _optional(data, "version", _text))
    if kind == "provider":
      scope = _mapping(data.get("scope"), "scope")
      return ProviderAssetReference(
        id=_text(data.get("id"), "id"),
        namespace=_text(data.get("namespace"), "namespace"),
        scope=ProviderAssetScope(
          provider=_text(scope.get("provider"), "provider"),
          account=_text(scope.get("account"), "account"),
          backend=_text(scope.get("backend"), "backend"),
          upstream_account_digest=_optional(scope, "upstream_account_digest", _text),
        ),
        expires_at=_optional(data, "expires_at", _integer),
      )
    raise ValueError(f"unsupported media source type '{kind}'")


@dataclass(slots=True)
class InlineSource(MediaSource):
  data: bytes = field(repr=False)
  kind: ClassVar[str] = "inline"

  def to_dict(self) -> dict[str, Any]:
    if not isinstance(self.data, bytes):
      raise TypeError("inline data must be immutable bytes")
    return {"type": self.kind, "data": base64.b64encode(self.data).decode("ascii")}


@dataclass(slots=True)
class RemoteSource(MediaSource):
  url: str = field(repr=False)
  kind: ClassVar[str] = "remote"


@dataclass(slots=True)
class AssetReference(MediaSource):
  id: str = field(repr=False)
  version: str | None = None
  kind: ClassVar[str] = "asset"


@dataclass(slots=True)
class ProviderAssetReference(MediaSource):
  id: str = field(repr=False)
  namespace: str
  scope: ProviderAssetScope
  expires_at: int | None = None
  kind: ClassVar[str] = "provider"


@dataclass(slots=True)
class MediaResource(_Value):
  source: MediaSource
  media_type: str | None = None
  digest: str | None = None

  @classmethod
  def inline(cls, media_type: str, data: bytes) -> MediaResource:
    if not isinstance(data, bytes):
      raise TypeError("inline data must be immutable bytes")
    return cls(InlineSource(data), media_type, hashlib.sha256(data).hexdigest())

  @classmethod
  def remote(cls, url: str, media_type: str | None = None) -> MediaResource:
    return cls(RemoteSource(url), media_type)

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> MediaResource:
    return cls(
      source=MediaSource.from_dict(_mapping(data.get("source"), "source")),
      media_type=_optional(data, "media_type", _text),
      digest=_optional(data, "digest", _text),
    )


@dataclass(slots=True)
class ImageDimensions(_Value):
  width: int
  height: int


class Content(_Value):
  __slots__ = ()
  kind: ClassVar[str]

  def to_dict(self, *, include_type: bool = True) -> dict[str, Any]:
    data = _Value.to_dict(self)
    return {"type": self.kind, **data} if include_type else data

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> Content:
    kind = data.get("type") if cls is Content else cls.kind
    if kind == "text":
      text_format = data.get("format", "plain")
      if text_format not in {"plain", "markdown"}:
        raise ValueError("unsupported text format")
      return TextContent(_text(data.get("text"), "text"), text_format)
    if kind == "transcript":
      segments = data.get("segments", [])
      if not isinstance(segments, list):
        raise TypeError("segments must be an array")
      parsed_segments = []
      previous_start = -1
      for item in segments:
        segment = _mapping(item, "segment")
        start = _integer(segment.get("start_ms"), "start_ms")
        end = _integer(segment.get("end_ms"), "end_ms")
        if end < start or start < previous_start:
          raise ValueError("invalid transcript timing")
        previous_start = start
        parsed_segments.append(
          TranscriptSegment(
            _text(segment.get("text"), "text"), start, end, _optional(segment, "speaker", _text)
          )
        )
      source = data.get("source")
      return TranscriptContent(
        _text(data.get("text"), "text"),
        parsed_segments,
        _optional(data, "language", _text),
        None if source is None else MediaResource.from_dict(_mapping(source, "source")),
      )
    if kind == "video":
      source = _mapping(data.get("source"), "video source")
      if source.get("type") == "resource":
        video_source = VideoSource(
          MediaResource.from_dict(_mapping(source.get("value"), "video resource"))
        )
      elif source.get("type") == "frames":
        frames = source.get("value")
        if not isinstance(frames, list) or not frames:
          raise ValueError("sampled video requires frames")
        parsed_frames = []
        previous_timestamp = -1
        for item in frames:
          frame_data = _mapping(item, "video frame")
          timestamp = _integer(frame_data.get("timestamp_ms"), "timestamp_ms")
          if timestamp <= previous_timestamp:
            raise ValueError("video timestamps must increase")
          previous_timestamp = timestamp
          parsed_frames.append(
            VideoFrame(
              timestamp, ImageContent.from_dict(_mapping(frame_data.get("image"), "frame image"))
            )
          )
        video_source = VideoSource(parsed_frames)
      else:
        raise ValueError("unsupported video source")
      return VideoContent(video_source, _dimensions(data), _optional(data, "duration_ms", _integer))
    resource = MediaResource.from_dict(_mapping(data.get("resource"), "resource"))
    if kind == "image":
      detail = data.get("detail")
      if detail not in {None, "auto", "low", "high", "original"}:
        raise ValueError("unsupported image detail")
      return ImageContent(resource, _dimensions(data), detail)
    if kind == "audio":
      audio_format = data.get("format")
      if audio_format not in {None, "wav", "mp3", "flac", "opus", "pcm16", "aac"}:
        raise ValueError("unsupported audio format")
      transcript = data.get("transcript")
      return AudioContent(
        resource,
        audio_format,
        _optional(data, "duration_ms", _integer),
        _optional(data, "sample_rate", lambda value, name: _integer(value, name, 1)),
        _optional(data, "channels", lambda value, name: _integer(value, name, 1)),
        None
        if transcript is None
        else TranscriptContent.from_dict(_mapping(transcript, "transcript")),
      )
    if kind == "document":
      return DocumentContent(
        resource,
        _optional(data, "filename", _text),
        _optional(data, "title", _text),
        _optional(data, "page_count", lambda value, name: _integer(value, name, 1)),
      )
    raise ValueError(f"unsupported content type '{kind}'")


def _dimensions(data: Mapping[str, Any]) -> ImageDimensions | None:
  value = data.get("dimensions")
  if value is None:
    return None
  value = _mapping(value, "dimensions")
  return ImageDimensions(
    _integer(value.get("width"), "width", 1), _integer(value.get("height"), "height", 1)
  )


@dataclass(slots=True)
class TextContent(Content):
  text: str
  format: Literal["plain", "markdown"] = "plain"
  kind: ClassVar[str] = "text"


@dataclass(slots=True)
class ImageContent(Content):
  resource: MediaResource
  dimensions: ImageDimensions | None = None
  detail: ImageDetail | None = None
  kind: ClassVar[str] = "image"


@dataclass(slots=True)
class TranscriptSegment(_Value):
  text: str
  start_ms: int
  end_ms: int
  speaker: str | None = None


@dataclass(slots=True)
class TranscriptContent(Content):
  text: str
  segments: list[TranscriptSegment] = field(default_factory=list)
  language: str | None = None
  source: MediaResource | None = None
  kind: ClassVar[str] = "transcript"


@dataclass(slots=True)
class AudioContent(Content):
  resource: MediaResource
  format: AudioFormat | None = None
  duration_ms: int | None = None
  sample_rate: int | None = None
  channels: int | None = None
  transcript: TranscriptContent | None = None
  kind: ClassVar[str] = "audio"


@dataclass(slots=True)
class VideoFrame(_Value):
  timestamp_ms: int
  image: ImageContent


@dataclass(slots=True)
class VideoSource(_Value):
  value: MediaResource | list[VideoFrame]

  def to_dict(self) -> dict[str, Any]:
    return {
      "type": "resource" if isinstance(self.value, MediaResource) else "frames",
      "value": _encode(self.value),
    }


@dataclass(slots=True)
class VideoContent(Content):
  source: VideoSource
  dimensions: ImageDimensions | None = None
  duration_ms: int | None = None
  kind: ClassVar[str] = "video"


@dataclass(slots=True)
class DocumentContent(Content):
  resource: MediaResource
  filename: str | None = None
  title: str | None = None
  page_count: int | None = None
  kind: ClassVar[str] = "document"


ContentBody = str | Sequence[Content]


def content_body_to_dict(value: ContentBody) -> str | list[dict[str, Any]]:
  if isinstance(value, str):
    return value
  if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
    raise TypeError("content must be text or a sequence of content parts")
  parts = []
  for item in value:
    if not isinstance(item, Content):
      raise TypeError("content parts must be Content values")
    parts.append(item.to_dict())
  return parts


def content_body_from_dict(value: object) -> ContentBody:
  if isinstance(value, str):
    return value
  if not isinstance(value, list):
    raise TypeError("content must be text or an array of content parts")
  return [Content.from_dict(_mapping(item, "content part")) for item in value]


@dataclass(slots=True)
class AudioOutputOptions(_Value):
  voice: str
  format: AudioFormat

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> AudioOutputOptions:
    voice = _text(data.get("voice"), "voice")
    audio_format = data.get("format")
    if not voice.strip() or audio_format not in {"wav", "mp3", "flac", "opus", "pcm16", "aac"}:
      raise ValueError("audio output requires a voice and supported format")
    return cls(voice, audio_format)


@dataclass(slots=True)
class ImageGenerationOptions(_Value):
  model: str | None = None
  size: str | None = None
  quality: str | None = None
  output_format: Literal["png", "jpeg", "webp"] = "png"
  background: str | None = None
  partial_images: int | None = None

  @classmethod
  def from_dict(cls, data: Mapping[str, Any]) -> ImageGenerationOptions:
    output_format = data.get("output_format", "png")
    if output_format not in {"png", "jpeg", "webp"}:
      raise ValueError("unsupported image output format")
    partial_images = _optional(data, "partial_images", _integer)
    if partial_images is not None and partial_images > 3:
      raise ValueError("partial_images must be at most 3")
    return cls(
      _optional(data, "model", _text),
      _optional(data, "size", _text),
      _optional(data, "quality", _text),
      output_format,
      _optional(data, "background", _text),
      partial_images,
    )
