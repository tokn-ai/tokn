from __future__ import annotations

import unittest

from tokn_requests import (
  AudioContent,
  AudioDelta,
  AudioOutputOptions,
  Content,
  ContentCompleted,
  DocumentContent,
  GenerateEvent,
  GenerateRequest,
  GenerateResponse,
  ImageContent,
  ImageGenerationOptions,
  ImagePreview,
  InlineSource,
  MediaResource,
  Message,
  TextContent,
  TranscriptContent,
  TranscriptSegment,
  VideoContent,
  VideoFrame,
  VideoSource,
)


class ContentTests(unittest.TestCase):
  def test_mixed_content_round_trip_and_snapshot(self) -> None:
    image = ImageContent(MediaResource.inline("image/png", b"image"))
    parts = [
      TextContent("Describe these inputs"),
      image,
      DocumentContent(
        MediaResource.remote("https://example.invalid/document.pdf"), filename="document.pdf"
      ),
      AudioContent(MediaResource.inline("audio/wav", b"audio"), format="wav"),
      VideoContent(VideoSource([VideoFrame(100, image)])),
      TranscriptContent("speech", [TranscriptSegment("speech", 0, 100, "speaker-1")]),
    ]
    request = (
      GenerateRequest.builder("media")
      .user(parts)
      .audio_output(AudioOutputOptions("voice", "wav"))
      .build()
    )
    restored = GenerateRequest.from_json(request.to_json())
    self.assertEqual(restored, request)
    self.assertEqual(restored.messages[0].content, parts)
    parts.clear()
    self.assertEqual(len(request.messages[0].content), 6)
    image_request = (
      GenerateRequest.builder("images")
      .prompt("draw")
      .image_generation(ImageGenerationOptions(output_format="webp", partial_images=2))
      .build()
    )
    self.assertEqual(image_request.to_dict()["image_generation"]["output_format"], "webp")
    self.assertEqual(Message.user("legacy").to_dict()["content"], "legacy")

  def test_generated_content_and_events_keep_media(self) -> None:
    image = ImageContent(MediaResource.inline("image/png", b"image"))
    response = GenerateResponse(http_status=200, headers={}, text="", raw={}, content=[image])
    self.assertEqual(GenerateResponse.from_json(response.to_json()), response)
    for event in [
      ContentCompleted(2, image),
      ImagePreview(2, 1, image),
      AudioDelta(0, "YQ==", "audio-id", "hello"),
    ]:
      self.assertEqual(GenerateEvent.from_json(event.to_json()), event)
    with self.assertRaises(ValueError):
      GenerateEvent.from_dict(
        {"type": "content_completed", "index": -1, "content": image.to_dict()}
      )

  def test_resources_and_timing_are_validated_and_payloads_redacted(self) -> None:
    resource = MediaResource.inline("image/png", b"private-payload")
    self.assertNotIn("private-payload", repr(resource))
    self.assertIsInstance(resource.source, InlineSource)
    with self.assertRaises(ValueError):
      Content.from_dict(
        {
          "type": "transcript",
          "text": "speech",
          "segments": [{"text": "speech", "start_ms": 10, "end_ms": 5}],
        }
      )
    with self.assertRaises(ValueError):
      GenerateRequest.builder("media").system([ImageContent(resource)]).user("describe").build()


if __name__ == "__main__":
  unittest.main()
