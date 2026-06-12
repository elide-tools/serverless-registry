import { Router } from "itty-router";
import { BlobUnknownError, ManifestUnknownError } from "./v2-errors";
import { InternalError, ServerError } from "./errors";
import { errorString, jsonHeaders, wrap } from "./utils";
import { hexToDigest } from "./user";
import { ManifestTagsListTooBigError } from "./v2-responses";
import { Env } from "..";
import {
  MINIMUM_CHUNK,
  MAXIMUM_CHUNK,
  MAXIMUM_CHUNK_UPLOAD_SIZE,
} from "./chunk";
import {
  CheckLayerResponse,
  CheckManifestResponse,
  FinishedUploadObject,
  GetLayerResponse,
  GetManifestResponse,
  PutManifestResponse,
  RegistryError,
  UploadObject,
  registries,
} from "./registry/registry";
import { RegistryHTTPClient } from "./registry/http";

const v2Router = Router({ base: "/v2/" });

v2Router.get("/", async (_req, _env: Env) => {
  return new Response();
});

v2Router.get("/_catalog", async (req, env: Env) => {
  const { n, last } = req.query;
  const response = await env.REGISTRY_CLIENT.listRepositories(
    n ? parseInt(n?.toString()) : undefined,
    last?.toString(),
  );
  if ("response" in response) {
    return response.response;
  }

  const url = new URL(req.url);
  return new Response(
    JSON.stringify({
      repositories: response.repositories,
    }),
    {
      headers: {
        Link: `${url.protocol}//${url.hostname}${url.pathname}?n=${n ?? 1000}&last=${response.cursor ?? ""}; rel=next`,
        "Content-Type": "application/json",
      },
    },
  );
});

v2Router.delete("/:name+/manifests/:reference", async (req, env: Env) => {
  // deleting a manifest works by retrieving the """main""" manifest that its key is a sha,
  // and then going through every tag and removing it
  //
  // after removing every tag, it's safe to remove the main manifest.
  //
  // if the transaction ends in an inconsistent state, the client can call this endpoint again
  // and we would try to delete everything again
  //
  // we limit 1k tag deletions per request. If more we will return an error so client retries.
  //
  // If somehow we need to remove by paginating, we accept a last query param

  const { last, limit } = req.query;
  const { name, reference } = req.params;
  // Reference is ALWAYS a sha256
  const manifest = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  if (manifest === null) {
    return new Response(JSON.stringify(ManifestUnknownError(reference)), {
      status: 404,
      headers: jsonHeaders(),
    });
  }
  const limitInt = parseInt(limit?.toString() ?? "1000", 10);
  const tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: isNaN(limitInt) ? 1000 : limitInt,
    cursor: last?.toString(),
  });
  for (const tag of tags.objects) {
    if (!tag.checksums.sha256) {
      continue;
    }

    if (hexToDigest(tag.checksums.sha256) === reference) {
      await env.REGISTRY.delete(tag.key);
    }
  }

  const url = new URL(req.url);
  if (tags.truncated) {
    url.searchParams.set("last", tags.truncated ? tags.cursor : "");
    return new Response(JSON.stringify(ManifestTagsListTooBigError), {
      status: 400,
      headers: {
        Link: `${url.toString()}; rel=next`,
        "Content-Type": "application/json",
      },
    });
  }

  // Last but not least, delete the digest manifest
  await env.REGISTRY.delete(`${name}/manifests/${reference}`);
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
    },
  });
});

// A `sha256:` reference is immutable content; anything else is a mutable tag.
function isTagReference(reference: string): boolean {
  return !reference.startsWith("sha256:");
}

// Build the body response for a resolved manifest (GET).
function manifestResponse(manifest: GetManifestResponse): Response {
  return new Response(manifest.stream, {
    headers: {
      "Content-Length": manifest.size.toString(),
      "Content-Type": manifest.contentType,
      "Docker-Content-Digest": manifest.digest,
    },
  });
}

// Build the headers-only response for a resolved manifest (HEAD).
function manifestHeadResponse(manifest: {
  size: number;
  digest: string;
  contentType: string;
}): Response {
  return new Response(null, {
    headers: {
      "Content-Length": manifest.size.toString(),
      "Content-Type": manifest.contentType,
      "Docker-Content-Digest": manifest.digest,
    },
  });
}

// Persist a manifest fetched from a fallback into R2 so the next pull is served locally.
async function storeManifest(
  env: Env,
  name: string,
  reference: string,
  stream: ReadableStream,
  contentType: string,
): Promise<void> {
  const [putResponse, err] = await wrap(
    env.REGISTRY_CLIENT.putManifest(name, reference, stream, {
      contentType,
      checkLayers: false,
    }),
  );
  if (err) {
    console.error(
      "Error syncing manifest",
      reference,
      "into registry:",
      errorString(err),
    );
    return;
  }

  if (putResponse && "response" in putResponse) {
    console.error(
      "Error syncing manifest (non-200 status):",
      putResponse.response.status,
    );
  }
}

// Seconds a cached mutable tag may be served from R2 without revalidating against the fallback.
// 0 (default) = always revalidate; a positive value bounds upstream cost (pull-through TTL).
function tagRevalidationTtlMs(env: Env): number {
  const seconds = parseInt(env.MANIFEST_TAG_TTL_SECONDS ?? "", 10);
  return Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : 0;
}

// True when a cached tag manifest is younger than the revalidation TTL, so it can be served
// straight from R2 without an upstream check.
async function tagCacheIsFresh(
  env: Env,
  name: string,
  reference: string,
): Promise<boolean> {
  const ttlMs = tagRevalidationTtlMs(env);
  if (ttlMs === 0) return false;
  const head = await env.REGISTRY.head(`${name}/manifests/${reference}`);
  return head !== null && Date.now() - head.uploaded.getTime() < ttlMs;
}

v2Router.head(
  "/:name+/manifests/:reference",
  async (req, env: Env, context: ExecutionContext) => {
    const { name, reference } = req.params;
    const res = await env.REGISTRY_CLIENT.manifestExists(name, reference);
    const registryList = registries(env);
    const cached = "exists" in res && res.exists ? res : null;

    // Immutable digests, and any reference with no fallback configured, are answered from R2. A
    // mutable tag with a fallback is revalidated below so a moved upstream tag is never reported
    // from a frozen R2 copy.
    if (
      cached !== null &&
      (!isTagReference(reference) || registryList.length === 0)
    ) {
      return manifestHeadResponse(cached);
    }

    // Within the revalidation TTL, serve the cached tag without an upstream check.
    if (cached !== null && (await tagCacheIsFresh(env, name, reference))) {
      return manifestHeadResponse(cached);
    }

    let checkManifestResponse: CheckManifestResponse | null = cached;
    for (const registry of registryList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.manifestExists(name, reference);
      // A 200 without a Docker-Content-Digest header (the header is SHOULD, not MUST) gives no
      // digest to compare or fetch by, so treat it as "the fallback can't answer".
      if ("response" in response || !response.exists || !response.digest) {
        // Fallback unreachable or can't give a digest: keep the cached answer.
        continue;
      }

      if (cached !== null && cached.digest === response.digest) {
        checkManifestResponse = cached; // cache is up to date
        break;
      }

      // Reference absent or stale in R2: report the upstream digest and refresh R2 in the
      // background so the subsequent pull is served locally.
      checkManifestResponse = {
        exists: true,
        size: response.size,
        digest: response.digest,
        contentType: response.contentType,
      };
      context.waitUntil(
        (async () => {
          const fresh = await client.getManifest(name, response.digest);
          if ("response" in fresh) {
            console.warn(
              "Can't sync with fallback registry because it has returned an error:",
              fresh.response.status,
            );
            return;
          }

          await storeManifest(
            env,
            name,
            reference,
            fresh.stream,
            fresh.contentType,
          );
        })(),
      );
      break;
    }

    if (checkManifestResponse === null || !checkManifestResponse.exists)
      return new Response(JSON.stringify(ManifestUnknownError(reference)), {
        status: 404,
        headers: jsonHeaders(),
      });

    return manifestHeadResponse(checkManifestResponse);
  },
);

v2Router.get(
  "/:name+/manifests/:reference",
  async (req, env: Env, context: ExecutionContext) => {
    const { name, reference } = req.params;
    const res = await env.REGISTRY_CLIENT.getManifest(name, reference);
    const registriesList = registries(env);

    if (!("response" in res)) {
      // Immutable digests, and any reference with no fallback configured, are served from R2. A
      // mutable tag with a fallback is revalidated against it (the source of truth for tags) so a
      // tag moved upstream (e.g. :latest) is never served from a frozen R2 copy.
      if (!isTagReference(reference) || registriesList.length === 0) {
        return manifestResponse(res);
      }

      // Within the revalidation TTL, serve the cached tag without an upstream check.
      if (await tagCacheIsFresh(env, name, reference)) {
        return manifestResponse(res);
      }

      for (const registry of registriesList) {
        const client = new RegistryHTTPClient(env, registry);
        const upstream = await client.manifestExists(name, reference);
        // A 200 without a Docker-Content-Digest header (the header is SHOULD, not MUST) gives no
        // digest to compare or fetch by, so treat it as "the fallback can't answer".
        if ("response" in upstream || !upstream.exists || !upstream.digest) {
          // Fallback unreachable or can't give a digest: keep serving the cached copy.
          continue;
        }

        if (upstream.digest === res.digest) {
          return manifestResponse(res); // cache is up to date
        }

        // Tag moved upstream: fetch the current manifest, refresh R2, serve the fresh copy.
        const fresh = await client.getManifest(name, upstream.digest);
        if ("response" in fresh) {
          continue;
        }

        res.stream.cancel().catch(() => {});
        const [serveStream, storeStream] = fresh.stream.tee();
        fresh.stream = serveStream;
        context.waitUntil(
          storeManifest(env, name, reference, storeStream, fresh.contentType),
        );
        return manifestResponse(fresh);
      }

      // No fallback could answer: serve the cached copy rather than failing the pull.
      return manifestResponse(res);
    }

    // R2 miss: fetch from a fallback and copy into R2 (pull-through / migration behavior).
    let getManifestResponse: GetManifestResponse | null = null;
    for (const registry of registriesList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.getManifest(name, reference);
      if ("response" in response) {
        continue;
      }

      getManifestResponse = response;
      if (res.response.status !== 404) {
        // Don't cache over a non-404 R2 error.
        break;
      }

      const [serveStream, storeStream] = getManifestResponse.stream.tee();
      getManifestResponse.stream = serveStream;
      context.waitUntil(
        storeManifest(
          env,
          name,
          reference,
          storeStream,
          getManifestResponse.contentType,
        ),
      );
      break;
    }

    if (getManifestResponse === null)
      return new Response(JSON.stringify(ManifestUnknownError(reference)), {
        status: 404,
        headers: jsonHeaders(),
      });

    return manifestResponse(getManifestResponse);
  },
);

v2Router.put("/:name+/manifests/:reference", async (req, env: Env) => {
  if (!req.headers.get("Content-Type")) {
    throw new ServerError("Content type not defined", 400);
  }

  const { name, reference } = req.params;
  const [res, err] = await wrap<PutManifestResponse | RegistryError, Error>(
    env.REGISTRY_CLIENT.putManifest(name, reference, req.body!, {
      contentType: req.headers.get("Content-Type")!,
    }),
  );
  if (err) {
    console.error("Error putting manifest:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      Location: res.location,
      "Docker-Content-Digest": res.digest,
    },
  });
});

v2Router.get(
  "/:name+/blobs/:digest",
  async (req, env: Env, context: ExecutionContext) => {
    const { name, digest } = req.params;
    const res = await env.REGISTRY_CLIENT.getLayer(name, digest);
    if (!("response" in res)) {
      return new Response(res.stream, {
        headers: {
          "Docker-Content-Digest": res.digest,
          "Content-Length": `${res.size}`,
        },
      });
    }

    let layerResponse: GetLayerResponse | null = null;
    const registriesList = registries(env);
    for (const registry of registriesList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.getLayer(name, digest);
      if ("response" in response) {
        continue;
      }

      layerResponse = response;
      const [s1, s2] = layerResponse.stream.tee();
      layerResponse.stream = s1;
      context.waitUntil(
        (async () => {
          const [response, err] = await wrap(
            env.REGISTRY_CLIENT.monolithicUpload(
              name,
              digest,
              s2,
              layerResponse.size,
            ),
          );
          if (err) {
            console.error(
              "Error uploading asynchronously the layer ",
              digest,
              "into main registry",
            );
            return;
          }

          if (response === false) {
            console.error(
              "Layer might be too big for the registry client",
              layerResponse.size,
            );
          }
        })(),
      );
      break;
    }

    if (layerResponse === null)
      return new Response(JSON.stringify(BlobUnknownError), { status: 404 });

    return new Response(layerResponse.stream, {
      headers: {
        "Docker-Content-Digest": layerResponse.digest,
        "Content-Length": `${layerResponse.size}`,
      },
    });
  },
);

v2Router.delete("/:name+/blobs/uploads/:id", async (req, env: Env) => {
  const { name, id } = req.params;
  const [res, err] = await wrap<true | RegistryError, Error>(
    env.REGISTRY_CLIENT.cancelUpload(name, id),
  );
  if (err) {
    console.error("Error cancelling upload:", errorString(err));
    return new InternalError();
  }

  if (res !== true && "response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 204,
    headers: { "Content-Length": "0" },
  });
});

// this is the first thing that the client asks for in an upload
v2Router.post("/:name+/blobs/uploads/", async (req, env: Env) => {
  const { name } = req.params;
  const { from, mount } = req.query;
  if (mount !== undefined && from !== undefined) {
    // Try to create a new upload from an existing layer on another repository
    const [finishedUploadObject, err] = await wrap<
      FinishedUploadObject | RegistryError,
      Error
    >(
      env.REGISTRY_CLIENT.mountExistingLayer(
        from.toString(),
        mount.toString(),
        name,
      ),
    );
    // If there is an error, fallback to the default layer upload system
    if (
      !(err || (finishedUploadObject && "response" in finishedUploadObject))
    ) {
      return new Response(null, {
        status: 201,
        headers: {
          "Content-Length": "0",
          Location: finishedUploadObject.location,
          "Docker-Content-Digest": finishedUploadObject.digest,
        },
      });
    }
  }
  // Upload a new layer
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.startUpload(name),
  );

  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  const range = `${uploadObject.range.join("-")}`;
  // Return a res with a Location header indicating where to send the data to complete the upload
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
      "Content-Range": range,
      Range: range,
      Location: uploadObject.location,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.get("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const [uploadObject, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.getUpload(name, uuid),
  );

  if (err) {
    return new InternalError();
  }

  if ("response" in uploadObject) {
    return uploadObject.response;
  }

  return new Response(null, {
    status: 204,
    headers: {
      Location: uploadObject.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      Range: `${uploadObject.range.join("-")}`,
      "Docker-Upload-UUID": uploadObject.id,
      "OCI-Chunk-Min-Length": `${Math.max(MINIMUM_CHUNK, uploadObject.minimumBytesPerChunk ?? MINIMUM_CHUNK)}`,
      "OCI-Chunk-Max-Length": `${Math.min(
        MAXIMUM_CHUNK_UPLOAD_SIZE,
        uploadObject.maximumBytesPerChunk ?? MAXIMUM_CHUNK,
      )}`,
    },
  });
});

v2Router.patch("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const contentRange = req.headers.get("Content-Range");
  const [start, end] = contentRange?.split("-") ?? [undefined, undefined];

  if (req.body == null) {
    return new Response(null, { status: 400 });
  }

  let contentLengthString = req.headers.get("Content-Length");
  let stream = req.body;
  if (!contentLengthString) {
    const blob = await req.blob();
    contentLengthString = `${blob.size}`;
    stream = blob.stream();
  }

  const url = new URL(req.url);
  const [res, err] = await wrap<UploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.uploadChunk(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      stream,
      +contentLengthString,
      end !== undefined && start !== undefined ? [+start, +end] : undefined,
    ),
  );
  if (err) {
    console.error("Uploading chunk:", errorString(err));
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  // Return a res indicating that the chunk was successfully uploaded
  return new Response(null, {
    status: 202,
    headers: {
      Location: res.location,
      // Note that the HTTP Range header byte ranges are inclusive and that will be honored, even in non-standard use cases.
      Range: `${res.range.join("-")}`,
      "Docker-Upload-UUID": res.id,
    },
  });
});

v2Router.put("/:name+/blobs/uploads/:uuid", async (req, env: Env) => {
  const { name, uuid } = req.params;
  const { digest } = req.query;
  if (!digest || typeof digest !== "string") {
    throw new ServerError("missing 'digest' query parameter", 400);
  }

  const url = new URL(req.url);
  const [res, err] = await wrap<FinishedUploadObject | RegistryError, Error>(
    env.REGISTRY_CLIENT.finishUpload(
      name,
      uuid,
      url.pathname + "?" + url.searchParams.toString(),
      digest,
      req.body ?? undefined,
      +(req.headers.get("Content-Length") ?? "0"),
    ),
  );

  if (err) {
    return new InternalError();
  }

  if ("response" in res) {
    return res.response;
  }

  return new Response(null, {
    status: 201,
    headers: {
      "Content-Length": "0",
      "Docker-Content-Digest": res.digest,
      Location: res.location,
    },
  });
});

v2Router.head("/:name+/blobs/:tag", async (req, env: Env) => {
  const { name, tag } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${tag}`);
  let layerExistsResponse: CheckLayerResponse | null = null;
  if (!res) {
    const registryList = registries(env);
    for (const registry of registryList) {
      const client = new RegistryHTTPClient(env, registry);
      const response = await client.layerExists(name, tag);
      if ("response" in response) {
        continue;
      }

      if (response.exists) {
        layerExistsResponse = response;
        break;
      }
    }

    if (layerExistsResponse === null || !layerExistsResponse.exists)
      return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  } else {
    if (res.checksums.sha256 === null) {
      throw new ServerError("invalid checksum from R2 backend");
    }

    layerExistsResponse = {
      digest: hexToDigest(res.checksums.sha256!),
      size: res.size,
      exists: true,
    };
  }

  return new Response(null, {
    headers: {
      "Content-Length": layerExistsResponse.size.toString(),
      "Docker-Content-Digest": layerExistsResponse.digest,
    },
  });
});

export type TagsList = {
  name: string;
  tags: string[];
};

v2Router.get("/:name+/tags/list", async (req, env: Env) => {
  const { name } = req.params;

  const { n: nStr = 50, last } = req.query;
  const n = +nStr;
  if (isNaN(n) || n <= 0) {
    throw new ServerError("invalid 'n' parameter", 400);
  }

  let tags = await env.REGISTRY.list({
    prefix: `${name}/manifests`,
    limit: n,
    startAfter: last ? `${name}/manifests/${last}` : undefined,
  });
  // Filter out sha256 manifest
  let manifestTags = tags.objects.filter(
    (tag) => !tag.key.startsWith(`${name}/manifests/sha256:`),
  );
  // If results are truncated and the manifest filter removed some result, extend the search to reach the n number of results expected by the client
  while (
    tags.objects.length > 0 &&
    tags.truncated &&
    manifestTags.length !== n
  ) {
    tags = await env.REGISTRY.list({
      prefix: `${name}/manifests`,
      limit: n - manifestTags.length,
      cursor: tags.cursor,
    });
    // Filter out sha256 manifest
    manifestTags = manifestTags.concat(
      tags.objects.filter(
        (tag) => !tag.key.startsWith(`${name}/manifests/sha256:`),
      ),
    );
  }

  const keys = manifestTags.map((object) => object.key.split("/").pop()!);
  const url = new URL(req.url);
  url.searchParams.set("n", `${n}`);
  url.searchParams.set("last", keys.length ? keys[keys.length - 1] : "");
  const responseHeaders: { "Content-Type": string; Link?: string } = {
    "Content-Type": "application/json",
  };
  // Only supply a next link if the previous result is truncated
  if (tags.truncated) {
    responseHeaders.Link = `${url.toString()}; rel=next`;
  }
  return new Response(
    JSON.stringify({
      name,
      tags: keys,
    }),
    {
      status: 200,
      headers: responseHeaders,
    },
  );
});

v2Router.delete("/:name+/blobs/:digest", async (req, env: Env) => {
  const { name, digest } = req.params;

  const res = await env.REGISTRY.head(`${name}/blobs/${digest}`);

  if (!res) {
    return new Response(JSON.stringify(BlobUnknownError), { status: 404 });
  }

  await env.REGISTRY.delete(`${name}/blobs/${digest}`);
  return new Response(null, {
    status: 202,
    headers: {
      "Content-Length": "0",
    },
  });
});

v2Router.post("/:name+/gc", async (req, env: Env) => {
  const { name } = req.params;

  const mode = req.query.mode ?? "unreferenced";
  if (mode !== "unreferenced" && mode !== "untagged") {
    throw new ServerError(
      "Mode must be either 'unreferenced' or 'untagged'",
      400,
    );
  }
  const result = await env.REGISTRY_CLIENT.garbageCollection(name, mode);
  return new Response(JSON.stringify({ success: result }));
});

export default v2Router;
