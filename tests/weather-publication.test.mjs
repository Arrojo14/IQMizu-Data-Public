import test from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { Readable } from "node:stream";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { decodeWeatherBundle, installWeatherFiles, sha256, validateWeatherFiles } from "../scripts/weather-publication.mjs";
import { parsePublishCommand, publishWebsiteData } from "../scripts/publish-website-data.mjs";
import { verifyWebsiteWeather } from "../scripts/verify-website.mjs";

const now = new Date("2026-09-06T12:00:00Z");
const recentName = "aemet-recent-climate-30.json";
function weather() {
  const files = {};
  const data = {};
  for (let i = 0; i < 300; i++) {
    data[`S${i}`] = ["2026-08-30", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05"]
      .map((fecha) => ({ fecha, precipitacion: 1, temperaturaMedia: 22, humedadMedia: 50 }));
    files[`aemet-monthly-S${i}.json`] = { timestamp: +now, data: Array.from({ length: 12 }, (_, m) => {
      const mes = new Date(Date.UTC(2025, 9 + m, 1)).toISOString().slice(0, 7);
      return { mes, etiqueta: mes, precipitacion: 10 };
    }) };
  }
  files[recentName] = { timestamp: +now, rangeDays: 30, data };
  return files;
}
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "iqmizu-weather-test-"));
  t.after(() => {
    assert.ok(resolve(root).startsWith(`${resolve(tmpdir())}${sep}iqmizu-weather-test-`));
    rmSync(root, { recursive: true, force: true });
  });
  const app = join(root, "app");
  const data = join(root, "persistent");
  mkdirSync(app);
  mkdirSync(data);
  const dbPath = join(data, "embalses.db");
  writeFileSync(dbPath, "reservoir database must remain untouched");
  return { root: app, data, dbPath };
}

test("weather validation rejects stale, partial, implausible and arbitrary-path bundles before changing caches", (t) => {
  const { data, dbPath } = fixture(t);
  const cacheDir = join(data, "cache");
  const files = weather();
  installWeatherFiles(files, cacheDir, { now });
  const before = readFileSync(join(cacheDir, recentName));
  const mutations = [
    (f) => { f["../embalses.db"] = {}; },
    (f) => { delete f["aemet-monthly-S1.json"]; },
    (f) => { f[recentName].timestamp = +new Date("2026-08-01"); },
    (f) => { f[recentName].data.S1[0].precipitacion = -1; },
    (f) => { f[recentName].data.S1[0].humedadMedia = 200; },
    (f) => { f[recentName].data.S1[0].fecha = "2026-02-30"; },
    (f) => { f[recentName].data.S1[0].fecha = "2026-10-01"; },
    (f) => { f[recentName].data.S1 = []; },
    (f) => { f["aemet-monthly-S1.json"].data = []; },
  ];
  for (const mutate of mutations) {
    const invalid = structuredClone(files);
    mutate(invalid);
    assert.throws(() => installWeatherFiles(invalid, cacheDir, { now }));
    assert.deepEqual(readFileSync(join(cacheDir, recentName)), before);
  }
  const buffer = gzipSync(JSON.stringify(files));
  assert.throws(() => decodeWeatherBundle(buffer, "0".repeat(64), { now }), /SHA-256/);
  assert.throws(() => decodeWeatherBundle(buffer.subarray(0, 30), sha256(buffer.subarray(0, 30)), { now }));
  assert.equal(readFileSync(dbPath, "utf8"), "reservoir database must remain untouched");
});

test("weather delivery uses external persistent state, retries after restart failure and skips completed repeats", async (t) => {
  const { root, data, dbPath } = fixture(t);
  const buffer = gzipSync(JSON.stringify(weather()));
  const command = `publish-weather ${"a".repeat(40)} ${sha256(buffer)}`;
  assert.equal(parsePublishCommand(command).kind, "publish-weather");
  assert.throws(() => parsePublishCommand(`${command} ../outside`), /Only/);
  const publish = () => publishWebsiteData(command, { root, dbPath, now, weatherInput: Readable.from([buffer]) });
  writeFileSync(join(root, "tmp"), "blocked restart directory");
  await assert.rejects(publish(), /EEXIST|ENOTDIR/);
  assert.equal(existsSync(join(data, "last-weather-publication.json")), false);
  assert.equal(readdirSync(join(data, "cache")).length, 301);
  unlinkSync(join(root, "tmp"));
  assert.equal((await publish()).changes, 0);
  assert.ok(existsSync(join(data, "last-weather-publication.json")));
  const restarted = statSync(join(root, "tmp", "restart.txt")).mtimeMs;
  assert.equal((await publish()).changes, 0);
  assert.equal(statSync(join(root, "tmp", "restart.txt")).mtimeMs, restarted);
  assert.equal(existsSync(join(root, "data")), false);
  assert.equal(readFileSync(dbPath, "utf8"), "reservoir database must remain untouched");
});

test("weather verification checks the public recent summary and actual monthly responses", async () => {
  const files = weather();
  const expected = validateWeatherFiles(files, { now });
  const options = { now, attempts: 1, fetchImpl: async (url) => {
    const station = /\/estacion\/(S\d+)\/mensual/.exec(url)?.[1];
    return Response.json(station ? files[`aemet-monthly-${station}.json`].data : { weather: expected });
  } };
  await verifyWebsiteWeather(files, options);
  await assert.rejects(verifyWebsiteWeather(files, { ...options, fetchImpl: async () => Response.json({ weather: null }) }), /does not match/);
});

test("publication retains valid local stations absent from a changed inventory", (t) => {
  const { data } = fixture(t);
  const cacheDir = join(data, "cache");
  const old = weather();
  old[recentName].data.DORMANT = structuredClone(old[recentName].data.S1);
  old["aemet-monthly-DORMANT.json"] = structuredClone(old["aemet-monthly-S1.json"]);
  installWeatherFiles(old, cacheDir, { now });
  const incoming = weather();
  const result = installWeatherFiles(incoming, cacheDir, { now });
  assert.equal(result.stations, 301);
  assert.deepEqual(JSON.parse(readFileSync(join(cacheDir, recentName))).data.DORMANT, old[recentName].data.DORMANT);
  assert.deepEqual(JSON.parse(readFileSync(join(cacheDir, "aemet-monthly-DORMANT.json"))), old["aemet-monthly-DORMANT.json"]);
  assert.equal(Object.hasOwn(incoming[recentName].data, "DORMANT"), false, "do not mutate the source bundle");
  assert.equal(installWeatherFiles(incoming, cacheDir, { now }).changes, 0);
});
