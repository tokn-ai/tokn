import { RequestError } from "./errors.js";

export type AudioFormat = "wav" | "mp3" | "flac" | "opus" | "pcm16" | "aac";
export type ImageDetail = "auto" | "low" | "high" | "original";
export interface ImageDimensions {
  readonly width: number;
  readonly height: number;
}
export interface AssetReference {
  readonly id: string;
  readonly version?: string;
}
export interface ProviderAssetScope {
  readonly provider: string;
  readonly account: string;
  readonly backend: string;
  readonly upstream_account_digest?: string;
}
export interface ProviderAssetReference {
  readonly id: string;
  readonly namespace: string;
  readonly scope: ProviderAssetScope;
  readonly expires_at?: number;
}
export type MediaSource =
  | { readonly type: "inline"; readonly data: string }
  | { readonly type: "remote"; readonly url: string }
  | ({ readonly type: "asset" } & AssetReference)
  | ({ readonly type: "provider" } & ProviderAssetReference);
export interface MediaResource {
  readonly source: MediaSource;
  readonly media_type?: string;
  readonly digest?: string;
}
export interface TextContent {
  readonly type: "text";
  readonly text: string;
  readonly format?: "plain" | "markdown";
}
export interface ImageContent {
  readonly type: "image";
  readonly resource: MediaResource;
  readonly dimensions?: ImageDimensions;
  readonly detail?: ImageDetail;
}
export interface TranscriptSegment {
  readonly text: string;
  readonly start_ms: number;
  readonly end_ms: number;
  readonly speaker?: string;
}
export interface TranscriptContent {
  readonly type: "transcript";
  readonly text: string;
  readonly segments?: readonly TranscriptSegment[];
  readonly language?: string;
  readonly source?: MediaResource;
}
export interface AudioContent {
  readonly type: "audio";
  readonly resource: MediaResource;
  readonly format?: AudioFormat;
  readonly duration_ms?: number;
  readonly sample_rate?: number;
  readonly channels?: number;
  readonly transcript?: Omit<TranscriptContent, "type">;
}
export interface VideoFrame {
  readonly timestamp_ms: number;
  readonly image: Omit<ImageContent, "type">;
}
export type VideoSource =
  | { readonly type: "resource"; readonly value: MediaResource }
  | { readonly type: "frames"; readonly value: readonly VideoFrame[] };
export interface VideoContent {
  readonly type: "video";
  readonly source: VideoSource;
  readonly dimensions?: ImageDimensions;
  readonly duration_ms?: number;
}
export interface DocumentContent {
  readonly type: "document";
  readonly resource: MediaResource;
  readonly filename?: string;
  readonly title?: string;
  readonly page_count?: number;
}
export type Content =
  | TextContent
  | ImageContent
  | AudioContent
  | VideoContent
  | DocumentContent
  | TranscriptContent;
export type ContentBody = string | readonly Content[];
export interface AudioOutputOptions {
  readonly voice: string;
  readonly format: AudioFormat;
}
export interface ImageGenerationOptions {
  readonly model?: string;
  readonly size?: string;
  readonly quality?: string;
  readonly output_format?: "png" | "jpeg" | "webp";
  readonly background?: string;
  readonly partial_images?: number;
}

type Failure = (message: string) => never;
const requestFailure: Failure = (message) => {
  throw new RequestError(message);
};

function object(
  value: unknown,
  name: string,
  fail: Failure,
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    fail(`${name} must be an object`);
  return value as Record<string, unknown>;
}
function text(
  value: unknown,
  name: string,
  fail: Failure,
  allow_empty = false,
): void {
  if (typeof value !== "string" || (!allow_empty && value["trim"]() === ""))
    fail(`${name} must be a string${allow_empty ? "" : " with content"}`);
}
function integer(
  value: unknown,
  name: string,
  fail: Failure,
  minimum = 0,
): void {
  if (!Number.isSafeInteger(value) || (value as number) < minimum)
    fail(`${name} must be a safe integer greater than or equal to ${minimum}`);
}
function optionalText(
  value: Record<string, unknown>,
  keys: readonly string[],
  name: string,
  fail: Failure,
): void {
  for (const key of keys)
    if (value[key] !== undefined) text(value[key], `${name}.${key}`, fail);
}
function optionalIntegers(
  value: Record<string, unknown>,
  keys: readonly string[],
  name: string,
  fail: Failure,
  minimum = 0,
): void {
  for (const key of keys)
    if (value[key] !== undefined)
      integer(value[key], `${name}.${key}`, fail, minimum);
}
function digest(value: unknown, name: string, fail: Failure): void {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value))
    fail(`${name} must be a lowercase SHA-256 digest`);
}
function resource(value: unknown, name: string, fail: Failure): void {
  const media = object(value, name, fail);
  optionalText(media, ["media_type"], name, fail);
  if (media["digest"] !== undefined)
    digest(media["digest"], `${name}.digest`, fail);
  const source = object(media["source"], `${name}.source`, fail);
  switch (source["type"]) {
    case "inline":
      text(source["data"], `${name}.source["data"]`, fail);
      if (
        !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
          source["data"] as string,
        )
      )
        fail(`${name}.source["data"] must be base64`);
      break;
    case "remote": {
      text(source["url"], `${name}.source["url"]`, fail);
      let url: URL;
      try {
        url = new URL(source["url"] as string);
      } catch {
        return fail(`${name}.source["url"] must be an HTTP or HTTPS URL`);
      }
      if (
        !["http:", "https:"].includes(url.protocol) ||
        url.username ||
        url.password
      )
        fail(`${name}.source["url"] must be HTTP or HTTPS without credentials`);
      break;
    }
    case "asset":
      text(source["id"], `${name}.source["id"]`, fail);
      optionalText(source, ["version"], `${name}.source`, fail);
      break;
    case "provider": {
      text(source["id"], `${name}.source["id"]`, fail);
      text(source["namespace"], `${name}.source["namespace"]`, fail);
      const scope = object(source["scope"], `${name}.source["scope"]`, fail);
      text(scope["provider"], `${name}.source["scope"].provider`, fail);
      text(scope["account"], `${name}.source["scope"].account`, fail);
      digest(scope["backend"], `${name}.source["scope"].backend`, fail);
      if (scope["upstream_account_digest"] !== undefined)
        digest(
          scope["upstream_account_digest"],
          `${name}.source["scope"].upstream_account_digest`,
          fail,
        );
      optionalIntegers(source, ["expires_at"], `${name}.source`, fail);
      break;
    }
    default:
      fail(`${name}.source has an unsupported type`);
  }
}
function image(
  value: Record<string, unknown>,
  name: string,
  fail: Failure,
): void {
  resource(value["resource"], `${name}.resource`, fail);
  if (value["dimensions"] !== undefined)
    dimensions(value["dimensions"], name, fail);
  if (
    value["detail"] !== undefined &&
    !["auto", "low", "high", "original"].includes(value["detail"] as string)
  )
    fail(`${name}.detail is unsupported`);
}
function dimensions(value: unknown, name: string, fail: Failure): void {
  const size = object(value, `${name}.dimensions`, fail);
  integer(size["width"], `${name}.dimensions.width`, fail, 1);
  integer(size["height"], `${name}.dimensions.height`, fail, 1);
}
function transcript(
  value: Record<string, unknown>,
  name: string,
  fail: Failure,
): void {
  text(value["text"], `${name}.text`, fail, true);
  optionalText(value, ["language"], name, fail);
  if (value["source"] !== undefined)
    resource(value["source"], `${name}.source`, fail);
  if (value["segments"] !== undefined) {
    if (!Array.isArray(value["segments"]))
      fail(`${name}.segments must be an array`);
    let previous_start = -1;
    for (const [index, item] of (value["segments"] as unknown[]).entries()) {
      const segment = object(item, `${name}.segments[${index}]`, fail);
      text(segment["text"], `${name}.segments[${index}].text`, fail, true);
      integer(segment["start_ms"], `${name}.segments[${index}].start_ms`, fail);
      integer(segment["end_ms"], `${name}.segments[${index}].end_ms`, fail);
      if (
        (segment["start_ms"] as number) < previous_start ||
        (segment["end_ms"] as number) < (segment["start_ms"] as number)
      )
        fail(`${name}.segments have invalid timing`);
      previous_start = segment["start_ms"] as number;
      optionalText(segment, ["speaker"], `${name}.segments[${index}]`, fail);
    }
  }
}

export function validateContent(
  value: unknown,
  name = "content",
  fail: Failure = requestFailure,
): asserts value is Content {
  const part = object(value, name, fail);
  switch (part["type"]) {
    case "text":
      text(part["text"], `${name}.text`, fail, true);
      if (
        part["format"] !== undefined &&
        part["format"] !== "plain" &&
        part["format"] !== "markdown"
      )
        fail(`${name}.format is unsupported`);
      break;
    case "image":
      image(part, name, fail);
      break;
    case "audio":
      resource(part["resource"], `${name}.resource`, fail);
      if (
        part["format"] !== undefined &&
        !["wav", "mp3", "flac", "opus", "pcm16", "aac"].includes(
          part["format"] as string,
        )
      )
        fail(`${name}.format is unsupported`);
      optionalIntegers(part, ["duration_ms"], name, fail);
      optionalIntegers(part, ["sample_rate", "channels"], name, fail, 1);
      if (part["transcript"] !== undefined)
        transcript(
          object(part["transcript"], `${name}.transcript`, fail),
          `${name}.transcript`,
          fail,
        );
      break;
    case "video": {
      const source = object(part["source"], `${name}.source`, fail);
      optionalIntegers(part, ["duration_ms"], name, fail);
      if (part["dimensions"] !== undefined)
        dimensions(part["dimensions"], name, fail);
      if (source["type"] === "resource")
        resource(source["value"], `${name}.source["value"]`, fail);
      else if (source["type"] === "frames") {
        if (!Array.isArray(source["value"]) || source["value"].length === 0)
          fail(`${name} requires video frames`);
        let previous_timestamp = -1;
        for (const item of source["value"] as unknown[]) {
          const frame = object(item, `${name} frame`, fail);
          integer(frame["timestamp_ms"], `${name} frame timestamp_ms`, fail);
          if (
            (frame["timestamp_ms"] as number) <= previous_timestamp ||
            (typeof part["duration_ms"] === "number" &&
              (frame["timestamp_ms"] as number) > part["duration_ms"])
          )
            fail(`${name} frames have invalid timing`);
          previous_timestamp = frame["timestamp_ms"] as number;
          image(
            object(frame["image"], `${name} frame image`, fail),
            `${name} frame image`,
            fail,
          );
        }
      } else fail(`${name}.source has an unsupported video representation`);
      break;
    }
    case "document":
      resource(part["resource"], `${name}.resource`, fail);
      optionalText(part, ["filename", "title"], name, fail);
      optionalIntegers(part, ["page_count"], name, fail, 1);
      break;
    case "transcript":
      transcript(part, name, fail);
      break;
    default:
      fail(`${name} has an unsupported content type`);
  }
}

export function validateContentBody(
  value: unknown,
  name = "content",
  fail: Failure = requestFailure,
): asserts value is ContentBody {
  if (typeof value === "string") return;
  if (!Array.isArray(value))
    fail(`${name} must be text or an array of content parts`);
  (value as unknown[]).forEach((part, index) =>
    validateContent(part, `${name}[${index}]`, fail),
  );
}

export function isEmptyContent(value: ContentBody): boolean {
  return typeof value === "string"
    ? value["length"] === 0
    : value["every"](
        (part) =>
          (part["type"] === "text" || part["type"] === "transcript") &&
          part["text"].length === 0,
      );
}
