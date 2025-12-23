import * as fs from "fs";
import * as path from "path";

const DEFAULT_SONGDATA_DB_PATH = "F:\\Games\\beatoraja - コピー\\songdata.db";
const DEFAULT_INSANE_TABLE_PATH = path.resolve("insane_data.json");
const DEFAULT_NORMAL_TABLE_PATH = path.resolve("normal_data.json");
const DEFAULT_OUTPUT_PATH = path.resolve("classified.json");
const DEFAULT_ANCHORS_PATH = path.resolve("anchors.json");

type SongRow = {
	path: string;
	title: string;
	subtitle?: string | null;
	genre?: string | null;
	artist?: string | null;
	subartist?: string | null;
	md5: string;
	sha256?: string | null;
	charthash?: string | null;
	content?: number | null;
	stagefile?: string | null;
	banner?: string | null;
	notes?: number | null;
};

type InsaneEntry = {
	level?: string | number | null;
	title?: string | null;
	md5?: string | null;
};

type NormalEntry = {
	level?: string | number | null;
	title?: string | null;
	md5?: string | null;
};

type BmsHeaders = {
	player?: number;
	rank?: number;
	total?: number;
	bpm?: number;
	lntype?: number;
};

type NoteEvent = {
	beat: number;
	time: number;
	lane: number; // 1..7 keys only
};

type AnalyzeResult = {
	song: SongRow;
	headers: BmsHeaders;
	durationSec: number;
	is7key: boolean;
	scratchCount: number;
	lnCount: number;
	features: DifficultyFeatures;
};

type DifficultyFeatures = {
	peak95: number;
	peak99: number;
	peakMax: number;
	avg: number;
	low10: number;
	densityVar: number;
	jackScore: number;
	chordScore: number;
	transitionEntropy: number;
	handBias: number;
	nps: number;
};

function assertNumber(x: unknown, fallback: number): number {
	return typeof x === "number" && Number.isFinite(x) ? x : fallback;
}

function clamp(x: number, min: number, max: number): number {
	return Math.max(min, Math.min(max, x));
}

function percentile(values: number[], p: number): number {
	if (values.length === 0) return 0;
	const sorted = [...values].sort((a, b) => a - b);
	const idx = (sorted.length - 1) * clamp(p, 0, 1);
	const lo = Math.floor(idx);
	const hi = Math.ceil(idx);
	if (lo === hi) return sorted[lo];
	const t = idx - lo;
	return sorted[lo] * (1 - t) + sorted[hi] * t;
}

function median(values: number[]): number {
	return percentile(values, 0.5);
}

function mean(values: number[]): number {
	if (values.length === 0) return 0;
	return values.reduce((a, b) => a + b, 0) / values.length;
}

function variance(values: number[]): number {
	if (values.length === 0) return 0;
	const m = mean(values);
	return mean(values.map((v) => (v - m) * (v - m)));
}

function normalizeMd5(md5: string | null | undefined): string | null {
	if (!md5) return null;
	const v = md5.trim().toLowerCase();
	return /^[0-9a-f]{32}$/.test(v) ? v : null;
}

function parseIntSafe(s: string | undefined | null, radix: number): number | null {
	if (typeof s !== "string") return null;
	const t = s.trim();
	if (t.length === 0) return null;
	const v = parseInt(t, radix);
	return Number.isFinite(v) ? v : null;
}

function parseFloatSafe(s: string | undefined | null): number | null {
	if (typeof s !== "string") return null;
	const t = s.trim();
	if (t.length === 0) return null;
	const v = parseFloat(t);
	return Number.isFinite(v) ? v : null;
}

type DifficultyAnchor = {
	path: string;
	star: number;
	weight?: number;
};

function readAnchors(anchorsPath: string): DifficultyAnchor[] {
	if (!fs.existsSync(anchorsPath)) return [];
	const raw = fs.readFileSync(anchorsPath);
	const txt = decodeTextSmart(raw);
	const parsed = JSON.parse(txt);
	if (!Array.isArray(parsed)) throw new Error("anchors.json must be an array");
	const anchors: DifficultyAnchor[] = [];
	for (const a of parsed) {
		if (!a || typeof a !== "object") continue;
		const p = String((a as any).path ?? "").trim();
		const star = Number((a as any).star);
		const weight = (a as any).weight === undefined ? undefined : Number((a as any).weight);
		if (!p) continue;
		if (!Number.isFinite(star)) continue;
		anchors.push({ path: p, star, weight: Number.isFinite(weight) ? weight : undefined });
	}
	return anchors;
}

function analyzeBmsPathForTraining(bmsPath: string): { features: DifficultyFeatures; durationSec: number } {
	const parsed = parseBmsFile(bmsPath);
	const tl = buildTimeline(parsed);
	const features = computeDifficultyFeatures(tl.notes, tl.durationSec);
	return { features, durationSec: tl.durationSec };
}

function decodeTextSmart(buf: Buffer): string {
	// UTF-8 BOM
	if (buf.length >= 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) {
		return buf.toString("utf8");
	}
	const utf8 = buf.toString("utf8");
	const replacementCount = (utf8.match(/\uFFFD/g) ?? []).length;
	if (replacementCount === 0) return utf8;
	// Fallback: many BMS files are Shift-JIS
	try {
		// eslint-disable-next-line @typescript-eslint/no-var-requires
		const iconv = require("iconv-lite");
		const sjis = iconv.decode(buf, "shift_jis");
		return sjis;
	} catch {
		return utf8;
	}
}

function buildSongDataObject(song: SongRow): Record<string, unknown> {
	const obj: Record<string, unknown> = {
		class: "bms.player.beatoraja.song.SongData",
		title: song.title,
		md5: song.md5,
	};
	if (song.subtitle) obj.subtitle = song.subtitle;
	if (song.genre) obj.genre = song.genre;
	if (song.artist) obj.artist = song.artist;
	if (song.subartist) obj.subartist = song.subartist;
	if (song.sha256) obj.sha256 = song.sha256;
	if (typeof song.content === "number") obj.content = song.content;
	if (song.stagefile) obj.stagefile = song.stagefile;
	if (song.banner) obj.banner = song.banner;
	if (song.charthash) obj.charthash = song.charthash;
	return obj;
}

async function readSongDataFromDb(dbPath: string): Promise<SongRow[]> {
	// eslint-disable-next-line @typescript-eslint/no-var-requires
	const sqlite3 = require("sqlite3");
	const db = new sqlite3.Database(dbPath);

	const allAsync = <T>(sql: string, params: unknown[] = []) =>
		new Promise<T[]>((resolve, reject) => {
			db.all(sql, params, (err: Error | null, rows: T[]) => {
				if (err) reject(err);
				else resolve(rows);
			});
		});

	try {
		const rows = await allAsync<any>(
			"SELECT path, title, subtitle, genre, artist, subartist, md5, sha256, charthash, content, stagefile, banner, notes FROM song"
		);
		return rows
			.map((r) => {
				const md5 = normalizeMd5(r.md5);
				if (!md5 || typeof r.path !== "string" || typeof r.title !== "string") return null;
				const song: SongRow = {
					path: r.path,
					title: r.title,
					subtitle: r.subtitle ?? null,
					genre: r.genre ?? null,
					artist: r.artist ?? null,
					subartist: r.subartist ?? null,
					md5,
					sha256: r.sha256 ?? null,
					charthash: r.charthash ?? null,
					content: typeof r.content === "number" ? r.content : null,
					stagefile: r.stagefile ?? null,
					banner: r.banner ?? null,
					notes: typeof r.notes === "number" ? r.notes : null,
				};
				return song;
			})
			.filter((x): x is SongRow => x !== null);
	} finally {
		db.close();
	}
}

function readInsaneTableLevels(insanePath: string): Map<string, number> {
	const buf = fs.readFileSync(insanePath);
	const text = decodeTextSmart(buf);
	const arr = JSON.parse(text) as InsaneEntry[];
	const map = new Map<string, number>();

	for (const e of arr) {
		const md5 = normalizeMd5(e.md5 ?? null);
		if (!md5) continue;
		const levelStr = typeof e.level === "number" ? String(e.level) : (e.level ?? "");
		const level = parseIntSafe(levelStr, 10);
		if (level === null) continue;
		if (level < 1 || level > 24) continue;
		map.set(md5, level);
	}
	return map;
}

const NORMAL_LEVELS: string[] = [
	"☆1",
	"☆2",
	"☆3",
	"☆4",
	"☆5",
	"☆6",
	"☆7",
	"☆8",
	"☆9",
	"☆10",
	"☆11",
	"☆11+",
	"☆12-",
	"☆12",
	"☆12+",
];

function normalizeNormalLevel(level: string | number | null | undefined): string | null {
	if (level === null || level === undefined) return null;
	const s = String(level).trim();
	if (!s) return null;
	if (s.startsWith("☆")) return s;
	if (/^\d+$/.test(s)) return `☆${s}`;
	if (/^\d+[+-]$/.test(s)) return `☆${s}`;
	return null;
}

function normalLevelRank(level: string | number | null | undefined): number | null {
	const norm = normalizeNormalLevel(level);
	if (!norm) return null;
	const idx = NORMAL_LEVELS.indexOf(norm);
	return idx >= 0 ? idx + 1 : null;
}

function normalRankToStar(rank: number, starMin: number, starMax: number): number {
	// Map rank=1..15 to starMin..starMax (default: -11..2)
	const r = clamp(rank, 1, NORMAL_LEVELS.length);
	const t = (r - 1) / Math.max(1, NORMAL_LEVELS.length - 1);
	return starMin * (1 - t) + starMax * t;
}

function readNormalTableLevels(normalPath: string): Map<string, number> {
	const buf = fs.readFileSync(normalPath);
	const text = decodeTextSmart(buf);
	const arr = JSON.parse(text) as NormalEntry[];
	if (!Array.isArray(arr)) throw new Error(`normal table must be array: ${normalPath}`);
	const map = new Map<string, number>();
	for (const e of arr) {
		const md5 = normalizeMd5(e.md5 ?? null);
		if (!md5) continue;
		const rank = normalLevelRank(e.level ?? null);
		if (!rank) continue;
		map.set(md5, rank);
	}
	return map;
}

function laneFromChannel(channel: number): number | null {
	// 7key: 11-15, 18-19
	if (channel >= 0x11 && channel <= 0x15) return channel - 0x10;
	if (channel === 0x18) return 6;
	if (channel === 0x19) return 7;
	return null;
}

function isScratchChannel(channel: number): boolean {
	return channel === 0x16;
}

function laneFromLnChannel(channel: number): { lane: number | null; isScratch: boolean } {
	// LN channels are visible channels + 0x40 (spec: 51-59 etc). We parse as hex.
	if (channel >= 0x51 && channel <= 0x59) {
		const base = channel - 0x40;
		const lane = laneFromChannel(base);
		const isScratch = isScratchChannel(base);
		return { lane, isScratch };
	}
	return { lane: null, isScratch: false };
}

type ParsedBms = {
	headers: BmsHeaders;
	measureScale: Map<number, number>;
	bpmExt: Map<string, number>; // id (2 chars) -> bpm
	stopExt: Map<string, number>; // id (2 chars) -> stopValue
	objects: Array<{ measure: number; channel: number; index: number; div: number }>;
	bpmEvents: Array<{ measure: number; index: number; div: number; bpm: number }>;
	stopEvents: Array<{ measure: number; index: number; div: number; stopValue: number }>;
};

function parseBmsFile(filePath: string): ParsedBms {
	const buf = fs.readFileSync(filePath);
	const text = decodeTextSmart(buf);
	const lines = text.split(/\r?\n/);

	const headers: BmsHeaders = {};
	const measureScale = new Map<number, number>();
	const bpmExt = new Map<string, number>();
	const stopExt = new Map<string, number>();

	const objects: ParsedBms["objects"] = [];
	const bpmEvents: ParsedBms["bpmEvents"] = [];
	const stopEvents: ParsedBms["stopEvents"] = [];
	const bpmRef: Array<{ measure: number; index: number; div: number; id: string }> = [];
	const stopRef: Array<{ measure: number; index: number; div: number; id: string }> = [];

	for (const rawLine of lines) {
		const line = rawLine.trim();
		if (line.length === 0) continue;
		const first = line[0];
		if (first === "*" || first === "%") continue;
		if (first !== "#") continue;

		// main data
		const mainMatch = line.match(/^#(\d{3})([0-9A-Za-z]{2}):(.*)$/);
		if (mainMatch) {
			const measure = parseInt(mainMatch[1], 10);
			const channel = parseInt(mainMatch[2], 16);
			const data = mainMatch[3].trim();

			if (channel === 0x02) {
				const v = parseFloatSafe(data);
				if (v !== null && v > 0) measureScale.set(measure, v);
				continue;
			}

			// array-based channels
			const div = Math.floor(data.length / 2);
			if (div <= 0) continue;

			for (let i = 0; i < div; i++) {
				const code = data.slice(i * 2, i * 2 + 2);
				if (code === "00") continue;

				if (channel === 0x03) {
					const bpm = parseIntSafe(code, 16);
					if (bpm !== null && bpm > 0) bpmEvents.push({ measure, index: i, div, bpm });
					continue;
				}

				if (channel === 0x08) {
					bpmRef.push({ measure, index: i, div, id: code.toUpperCase() });
					continue;
				}

				if (channel === 0x09) {
					stopRef.push({ measure, index: i, div, id: code.toUpperCase() });
					continue;
				}

				// Keep only channels needed for this app (keys/scratch/LN)
				const lane = laneFromChannel(channel);
				const isScratch = isScratchChannel(channel);
				const ln = laneFromLnChannel(channel);
				if (lane || isScratch || ln.lane || ln.isScratch) {
					objects.push({ measure, channel, index: i, div });
				}
			}
			continue;
		}

		// header (incl. extended definitions)
		const headerMatch = line.match(/^#([A-Za-z0-9]+)\s*(.*)$/);
		if (!headerMatch) continue;
		const key = headerMatch[1].toUpperCase();
		const value = (headerMatch[2] ?? "").trim();

		if (key === "PLAYER") {
			const v = parseIntSafe(value, 10);
			if (v !== null) headers.player = v;
			continue;
		}
		if (key === "RANK") {
			const v = parseIntSafe(value, 10);
			if (v !== null) headers.rank = v;
			continue;
		}
		if (key === "TOTAL") {
			const v = parseFloatSafe(value);
			if (v !== null) headers.total = v;
			continue;
		}
		if (key === "BPM") {
			const v = parseFloatSafe(value);
			if (v !== null) headers.bpm = v;
			continue;
		}
		if (key === "LNTYPE") {
			const v = parseIntSafe(value, 10);
			if (v !== null) headers.lntype = v;
			continue;
		}
		if (key.startsWith("BPM") && key.length === 5) {
			const id = key.slice(3, 5).toUpperCase();
			const v = parseFloatSafe(value);
			if (v !== null && v > 0) bpmExt.set(id, v);
			continue;
		}
		if (key.startsWith("STOP") && key.length === 6) {
			const id = key.slice(4, 6).toUpperCase();
			const v = parseFloatSafe(value);
			if (v !== null && v > 0) stopExt.set(id, v);
			continue;
		}
	}

	// Resolve delayed #BPMxx/#STOPxx references
	for (const r of bpmRef) {
		const bpm = bpmExt.get(r.id);
		if (typeof bpm === "number" && Number.isFinite(bpm) && bpm > 0) {
			bpmEvents.push({ measure: r.measure, index: r.index, div: r.div, bpm });
		}
	}
	for (const r of stopRef) {
		const stopValue = stopExt.get(r.id);
		if (typeof stopValue === "number" && Number.isFinite(stopValue) && stopValue > 0) {
			stopEvents.push({ measure: r.measure, index: r.index, div: r.div, stopValue });
		}
	}

	return { headers, measureScale, bpmExt, stopExt, objects, bpmEvents, stopEvents };
}

function buildMeasureStartBeats(maxMeasure: number, measureScale: Map<number, number>): number[] {
	const start: number[] = new Array(maxMeasure + 2).fill(0);
	let beat = 0;
	start[0] = 0;
	for (let m = 0; m <= maxMeasure + 1; m++) {
		start[m] = beat;
		const scale = measureScale.get(m) ?? 1;
		beat += 4 * scale;
	}
	return start;
}

function buildBeatPositionWithStart(
	measure: number,
	index: number,
	div: number,
	measureScale: Map<number, number>,
	measureStartBeats: number[]
): number {
	const scale = measureScale.get(measure) ?? 1;
	const measureBeats = 4 * scale;
	const frac = div > 0 ? index / div : 0;
	const startBeat = measureStartBeats[measure] ?? 0;
	return startBeat + frac * measureBeats;
}

type TimelineEvent =
	| { kind: "note"; beat: number; lane: number }
	| { kind: "bpm"; beat: number; bpm: number }
	| { kind: "stop"; beat: number; stopBeats: number };

function buildTimeline(parsed: ParsedBms): { notes: NoteEvent[]; durationSec: number; scratchCount: number; lnCount: number; is7key: boolean } {
	const baseBpm = parsed.headers.bpm ?? 130;
	const events: TimelineEvent[] = [];

	const maxMeasure = Math.max(
		0,
		...parsed.objects.map((o) => o.measure),
		...parsed.bpmEvents.map((e) => e.measure),
		...parsed.stopEvents.map((s) => s.measure)
	);
	const measureStartBeats = buildMeasureStartBeats(maxMeasure, parsed.measureScale);

	// bpm/stop
	for (const e of parsed.bpmEvents) {
		const beat = buildBeatPositionWithStart(e.measure, e.index, e.div, parsed.measureScale, measureStartBeats);
		events.push({ kind: "bpm", beat, bpm: e.bpm });
	}
	for (const s of parsed.stopEvents) {
		const beat = buildBeatPositionWithStart(s.measure, s.index, s.div, parsed.measureScale, measureStartBeats);
		const stopBeats = s.stopValue / 48; // see spec: 48 = 192/4
		if (stopBeats > 0) events.push({ kind: "stop", beat, stopBeats });
	}

	// notes & ln candidates
	let scratchCount = 0;
	const lnObjectsByLane = new Map<number, number[]>();
	const lnScratchBeats: number[] = [];
	const keyNoteBeats: Array<{ beat: number; lane: number }> = [];
	const scratchNoteBeats: number[] = [];

	for (const o of parsed.objects) {
		const beat = buildBeatPositionWithStart(o.measure, o.index, o.div, parsed.measureScale, measureStartBeats);
		const lane = laneFromChannel(o.channel);
		if (lane) {
			keyNoteBeats.push({ beat, lane });
			continue;
		}
		if (isScratchChannel(o.channel)) {
			scratchCount += 1;
			scratchNoteBeats.push(beat);
			continue;
		}
		const ln = laneFromLnChannel(o.channel);
		if (ln.lane) {
			const arr = lnObjectsByLane.get(ln.lane) ?? [];
			arr.push(beat);
			lnObjectsByLane.set(ln.lane, arr);
			continue;
		}
		if (ln.isScratch) {
			lnScratchBeats.push(beat);
			continue;
		}
	}

	// Pair LN objects (simple toggle pairing)
	let lnCount = 0;
	for (const [lane, beats] of lnObjectsByLane) {
		beats.sort((a, b) => a - b);
		// pair sequentially
		for (let i = 0; i + 1 < beats.length; i += 2) {
			lnCount += 1;
			// treat LN start as a note for difficulty
			keyNoteBeats.push({ beat: beats[i], lane });
		}
	}
	// scratch LN pairing contributes to lnCount too (for filtering)
	if (lnScratchBeats.length >= 2) {
		lnScratchBeats.sort((a, b) => a - b);
		lnCount += Math.floor(lnScratchBeats.length / 2);
		// LN scratch also counts as scratch actions (but spec filters scratch separately)
		scratchCount += Math.floor(lnScratchBeats.length / 2);
	}

	const is7key = keyNoteBeats.some((n) => n.lane === 6 || n.lane === 7);

	// Create timeline for time conversion
	for (const kn of keyNoteBeats) {
		events.push({ kind: "note", beat: kn.beat, lane: kn.lane });
	}
	// scratch affects duration estimation (but not key strain)
	for (const b of scratchNoteBeats) events.push({ kind: "note", beat: b, lane: 0 });

	events.sort((a, b) => {
		if (a.beat !== b.beat) return a.beat - b.beat;
		const prio = (e: TimelineEvent) => (e.kind === "bpm" ? 0 : e.kind === "stop" ? 1 : 2);
		return prio(a) - prio(b);
	});

	let bpm = baseBpm;
	let lastBeat = 0;
	let time = 0;
	const secPerBeat = () => 60 / bpm;

	const noteEvents: NoteEvent[] = [];
	let lastNoteTime = 0;

	for (const e of events) {
		const deltaBeat = e.beat - lastBeat;
		if (deltaBeat > 0) {
			time += deltaBeat * secPerBeat();
			lastBeat = e.beat;
		}

		if (e.kind === "bpm") {
			if (e.bpm > 0) bpm = e.bpm;
			continue;
		}
		if (e.kind === "stop") {
			time += e.stopBeats * secPerBeat();
			continue;
		}
		if (e.kind === "note") {
			lastNoteTime = Math.max(lastNoteTime, time);
			if (e.lane >= 1 && e.lane <= 7) noteEvents.push({ beat: e.beat, time, lane: e.lane });
			continue;
		}
	}

	const durationSec = lastNoteTime;
	return { notes: noteEvents, durationSec, scratchCount, lnCount, is7key };
}

function computeRawDifficulty(notes: NoteEvent[], durationSec: number): number {
	// deprecated: kept for compatibility (should not be used)
	return computeDifficultyFeatures(notes, durationSec).peak95;
}

function computeDifficultyFeatures(notes: NoteEvent[], durationSec: number): DifficultyFeatures {
	if (notes.length === 0) {
		return {
			peak95: 0,
			peak99: 0,
			peakMax: 0,
			avg: 0,
			low10: 0,
			densityVar: 0,
			jackScore: 0,
			chordScore: 0,
			transitionEntropy: 0,
			handBias: 0,
			nps: 0,
		};
	}

	const sorted = [...notes].sort((a, b) => a.time - b.time || a.lane - b.lane);
	const lanes = 7;
	const lastTimeByLane: Array<number | null> = new Array(lanes + 1).fill(null);
	const strainByLane = Array(lanes + 1).fill(0);
	const decay = 0.38; // seconds (slightly faster decay to emphasize peaks)

	// group chords (events within small epsilon)
	const chordEps = 0.004;
	let i = 0;

	// Section peaks approximate local "strain" peaks and recovery.
	const section = 0.35;
	let sectionStart = 0;
	let sectionPeak = 0;
	const sectionPeaks: number[] = [];

	let jackScore = 0;
	let chordScore = 0;

	// For transition entropy (pattern variability)
	const laneTransitions = new Map<string, number>();
	let prevLane: number | null = null;

	// For hand bias (counts)
	let left = 0;
	let right = 0;
	let center = 0;

	while (i < sorted.length) {
		const t = sorted[i].time;

		while (t >= sectionStart + section) {
			sectionPeaks.push(sectionPeak);
			sectionStart += section;
			sectionPeak = 0;
		}

		const chordLanes: number[] = [];
		let j = i;
		while (j < sorted.length && Math.abs(sorted[j].time - t) <= chordEps) {
			chordLanes.push(sorted[j].lane);
			j++;
		}

		const chordSize = chordLanes.length;
		if (chordSize >= 2) {
			// chord difficulty grows more than log for 7key clear (押し分け/運指)
			chordScore += Math.pow(chordSize - 1, 1.15);
		}

		for (let k = i; k < j; k++) {
			const lane = sorted[k].lane;

			// hand bias counts
			if (lane <= 3) left++;
			else if (lane >= 5) right++;
			else center++;

			if (prevLane !== null) {
				const key = `${prevLane}-${lane}`;
				laneTransitions.set(key, (laneTransitions.get(key) ?? 0) + 1);
			}
			prevLane = lane;

			const prev = lastTimeByLane[lane];
			const delta = prev === null ? 999 : Math.max(0.001, t - prev);

			const decayFactor = prev === null ? 0 : Math.exp(-delta / decay);
			// base strain contribution
			strainByLane[lane] = strainByLane[lane] * decayFactor + 1 / delta;
			lastTimeByLane[lane] = t;

			// jack penalty: very strong when repeated fast, and grows with repetition
			if (delta <= 0.14) {
				jackScore += Math.pow((0.14 - delta) / 0.14, 1.8);
			}
		}

		// overall local strain: sum of top-3 lanes (randomのため片手混ざりも重視) + chord bonus
		const laneStrains = strainByLane.slice(1);
		laneStrains.sort((a, b) => b - a);
		const top = laneStrains[0] + 0.75 * laneStrains[1] + 0.45 * laneStrains[2];
		const chordBonus = chordSize >= 2 ? 0.55 * Math.pow(chordSize - 1, 1.05) : 0;
		const totalStrain = top + chordBonus;
		sectionPeak = Math.max(sectionPeak, totalStrain);

		i = j;
	}
	sectionPeaks.push(sectionPeak);

	const peak95 = percentile(sectionPeaks, 0.95);
	const peak99 = percentile(sectionPeaks, 0.99);
	const peakMax = percentile(sectionPeaks, 1);
	const avg = mean(sectionPeaks);
	const low10 = percentile(sectionPeaks, 0.1);
	const densityVar = variance(sectionPeaks);
	const nps = notes.length / Math.max(1, durationSec);

	// entropy of lane transitions
	const totalTrans = [...laneTransitions.values()].reduce((a, b) => a + b, 0);
	let transitionEntropy = 0;
	if (totalTrans > 0) {
		for (const c of laneTransitions.values()) {
			const p = c / totalTrans;
			transitionEntropy -= p * Math.log2(p);
		}
	}

	const totalHand = left + right + center;
	const effLeft = left + 0.5 * center;
	const effRight = right + 0.5 * center;
	const handBias = totalHand > 0 ? Math.abs(effLeft - effRight) / totalHand : 0;

	return {
		peak95,
		peak99,
		peakMax,
		avg,
		low10,
		densityVar,
		jackScore,
		chordScore,
		transitionEntropy,
		handBias,
		nps,
	};
}

function isotonicIncreasing(points: Array<{ x: number; y: number }>): Array<{ x: number; y: number }> {
	// Pool Adjacent Violators (PAV) for non-decreasing y over increasing x.
	const sorted = [...points].sort((a, b) => a.x - b.x);
	type Block = { xMin: number; xMax: number; y: number; weight: number };
	const blocks: Block[] = [];
	for (const p of sorted) {
		blocks.push({ xMin: p.x, xMax: p.x, y: p.y, weight: 1 });
		while (blocks.length >= 2) {
			const b = blocks[blocks.length - 1];
			const a = blocks[blocks.length - 2];
			if (a.y <= b.y) break;
			const w = a.weight + b.weight;
			const y = (a.y * a.weight + b.y * b.weight) / w;
			blocks.splice(blocks.length - 2, 2, { xMin: a.xMin, xMax: b.xMax, y, weight: w });
		}
	}
	// Expand to point-per-x
	const out: Array<{ x: number; y: number }> = [];
	for (const b of blocks) {
		// We had one point per star level, so xMin==xMax; still keep generic.
		out.push({ x: b.xMin, y: b.y });
	}
	return out.sort((a, b) => a.x - b.x);
}

type RidgeModel = {
	means: number[];
	stds: number[];
	weights: number[]; // includes bias as weights[0]
};

function solveLinearSystem(a: number[][], b: number[]): number[] {
	// Gaussian elimination with partial pivoting
	const n = b.length;
	const m = a.map((row, i) => [...row, b[i]]);

	for (let col = 0; col < n; col++) {
		// pivot
		let pivot = col;
		for (let r = col + 1; r < n; r++) {
			if (Math.abs(m[r][col]) > Math.abs(m[pivot][col])) pivot = r;
		}
		if (Math.abs(m[pivot][col]) < 1e-12) continue;
		if (pivot !== col) {
			const tmp = m[col];
			m[col] = m[pivot];
			m[pivot] = tmp;
		}
		// normalize
		const div = m[col][col];
		for (let c = col; c <= n; c++) m[col][c] /= div;
		// eliminate
		for (let r = 0; r < n; r++) {
			if (r === col) continue;
			const factor = m[r][col];
			if (Math.abs(factor) < 1e-12) continue;
			for (let c = col; c <= n; c++) m[r][c] -= factor * m[col][c];
		}
	}

	const x = new Array(n).fill(0);
	for (let i = 0; i < n; i++) x[i] = m[i][n];
	return x;
}

function featureVector(f: DifficultyFeatures): number[] {
	// Keep noteCount out (TOTAL NOTESは当てにならない)。
	// NPSは軽く入れるが、ピーク重視で寄与は小。
	return [
		Math.log1p(f.peak95),
		Math.log1p(f.peak99),
		Math.log1p(f.peakMax),
		Math.log1p(f.avg),
		Math.log1p(f.low10 + 1e-6),
		Math.log1p(f.densityVar),
		Math.log1p(f.jackScore),
		Math.log1p(f.chordScore),
		f.transitionEntropy,
		f.handBias,
		Math.log1p(f.nps),
	];
}

function trainRidgeRegression(samples: Array<{ x: number[]; y: number; w: number }>, lambda = 2.5): RidgeModel {
	const d = samples[0]?.x.length ?? 0;
	const means = new Array(d).fill(0);
	const stds = new Array(d).fill(1);

	// weighted mean/std
	const sumW = samples.reduce((a, s) => a + s.w, 0);
	for (let j = 0; j < d; j++) {
		means[j] = samples.reduce((a, s) => a + s.w * s.x[j], 0) / Math.max(1e-9, sumW);
		const v = samples.reduce((a, s) => {
			const z = s.x[j] - means[j];
			return a + s.w * z * z;
		}, 0);
		stds[j] = Math.sqrt(v / Math.max(1e-9, sumW));
		if (!Number.isFinite(stds[j]) || stds[j] < 1e-6) stds[j] = 1;
	}

	const p = d + 1; // +bias
	const xtwx: number[][] = Array.from({ length: p }, () => new Array(p).fill(0));
	const xtwy: number[] = new Array(p).fill(0);

	for (const s of samples) {
		const z: number[] = new Array(p).fill(1);
		for (let j = 0; j < d; j++) z[j + 1] = (s.x[j] - means[j]) / stds[j];
		for (let r = 0; r < p; r++) {
			xtwy[r] += s.w * z[r] * s.y;
			for (let c = 0; c < p; c++) {
				xtwx[r][c] += s.w * z[r] * z[c];
			}
		}
	}

	// ridge (do not regularize bias)
	for (let i = 1; i < p; i++) xtwx[i][i] += lambda;
	const weights = solveLinearSystem(xtwx, xtwy);
	return { means, stds, weights };
}

function predictRidge(model: RidgeModel, x: number[]): number {
	const z0 = 1;
	let y = model.weights[0] * z0;
	for (let j = 0; j < x.length; j++) {
		const z = (x[j] - model.means[j]) / model.stds[j];
		y += model.weights[j + 1] * z;
	}
	return y;
}

type IsotonicBlock = { xMin: number; xMax: number; y: number; weight: number };

function isotonicBlocksIncreasing(points: Array<{ x: number; y: number; w: number }>): IsotonicBlock[] {
	const sorted = [...points].sort((a, b) => a.x - b.x);
	const blocks: IsotonicBlock[] = [];
	for (const p of sorted) {
		blocks.push({ xMin: p.x, xMax: p.x, y: p.y, weight: p.w });
		while (blocks.length >= 2) {
			const b = blocks[blocks.length - 1];
			const a = blocks[blocks.length - 2];
			if (a.y <= b.y) break;
			const w = a.weight + b.weight;
			const y = (a.y * a.weight + b.y * b.weight) / Math.max(1e-9, w);
			blocks.splice(blocks.length - 2, 2, { xMin: a.xMin, xMax: b.xMax, y, weight: w });
		}
	}
	return blocks;
}

function buildCalibrationMapper(pairs: Array<{ pred: number; truth: number; w: number }>): (pred: number) => number {
	if (pairs.length < 5) return (x) => x;
	const blocks = isotonicBlocksIncreasing(pairs.map((p) => ({ x: p.pred, y: p.truth, w: p.w })));
	const xs = blocks.map((b) => (b.xMin + b.xMax) / 2);
	const ys = blocks.map((b) => b.y);
	return (pred: number) => {
		if (!Number.isFinite(pred)) return ys[0];
		if (pred <= xs[0]) {
			const slope = (ys[1] - ys[0]) / Math.max(1e-9, xs[1] - xs[0]);
			return ys[0] + (pred - xs[0]) * slope;
		}
		if (pred >= xs[xs.length - 1]) {
			const n = xs.length;
			const slope = (ys[n - 1] - ys[n - 2]) / Math.max(1e-9, xs[n - 1] - xs[n - 2]);
			return ys[n - 1] + (pred - xs[n - 1]) * slope;
		}
		for (let i = 0; i < xs.length - 1; i++) {
			if (pred >= xs[i] && pred <= xs[i + 1]) {
				const t = (pred - xs[i]) / Math.max(1e-9, xs[i + 1] - xs[i]);
				return ys[i] * (1 - t) + ys[i + 1] * t;
			}
		}
		return ys[0];
	};
}

function buildRawToStarMapper(levelToRawSamples: Map<number, number[]>): (raw: number) => number {
	const levelPoints: Array<{ x: number; y: number }> = [];
	for (const [level, raws] of levelToRawSamples) {
		if (raws.length === 0) continue;
		levelPoints.push({ x: level, y: median(raws) });
	}
	if (levelPoints.length < 2) {
		// fallback: identity-ish
		return (raw) => raw;
	}

	// enforce monotonic median(raw) by level
	const mono = isotonicIncreasing(levelPoints);

	// Build piecewise-linear inverse: raw -> star
	const xs = mono.map((p) => p.x);
	const ys = mono.map((p) => p.y);

	return (raw: number) => {
		if (!Number.isFinite(raw)) return xs[0];
		if (raw <= ys[0]) {
			const slope = (xs[1] - xs[0]) / Math.max(1e-9, ys[1] - ys[0]);
			return xs[0] + (raw - ys[0]) * slope;
		}
		if (raw >= ys[ys.length - 1]) {
			const n = ys.length;
			const slope = (xs[n - 1] - xs[n - 2]) / Math.max(1e-9, ys[n - 1] - ys[n - 2]);
			return xs[n - 1] + (raw - ys[n - 1]) * slope;
		}
		for (let i = 0; i < ys.length - 1; i++) {
			if (raw >= ys[i] && raw <= ys[i + 1]) {
				const t = (raw - ys[i]) / Math.max(1e-9, ys[i + 1] - ys[i]);
				return xs[i] * (1 - t) + xs[i + 1] * t;
			}
		}
		return xs[0];
	};
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T, idx: number) => Promise<R>): Promise<R[]> {
	const results: R[] = new Array(items.length);
	let nextIndex = 0;
	const workers = Array.from({ length: Math.max(1, limit) }, async () => {
		while (true) {
			const idx = nextIndex;
			nextIndex++;
			if (idx >= items.length) return;
			results[idx] = await fn(items[idx], idx);
		}
	});
	await Promise.all(workers);
	return results;
}

function shouldAnalyzeByDbNotes(song: SongRow): boolean {
	const notes = song.notes ?? 0;
	return notes >= 1000 && notes < 5000;
}

function passesFilters(song: SongRow, res: AnalyzeResult): boolean {
	const notes = song.notes ?? 0;
	if (!(notes >= 1000 && notes < 5000)) return false;
	if ((res.headers.total ?? 0) < 250) return false;
	const rank = res.headers.rank ?? -1;
	if (!(rank === 2 || rank === 3)) return false;
	if ((res.headers.player ?? -1) !== 1) return false;
	if (!res.is7key) return false;
	if (res.scratchCount > notes / 20) return false;
	if (res.lnCount > notes / 20) return false;
	return true;
}

function findFreedomDiveFourDimensions(songs: SongRow[]): SongRow | null {
	const needle1 = "freedom dive";
	const needle2 = "four dimensions";
	const candidates = songs.filter((s) => {
		const t = (s.title ?? "").toLowerCase();
		const st = (s.subtitle ?? "").toLowerCase();
		return (t.includes(needle1) || st.includes(needle1)) && (t.includes(needle2) || st.includes(needle2));
	});
	if (candidates.length === 0) return null;
	// prefer chart with largest notes (usually harder)
	candidates.sort((a, b) => (b.notes ?? 0) - (a.notes ?? 0));
	return candidates[0];
}

async function analyzeSong(song: SongRow): Promise<AnalyzeResult> {
	const parsed = parseBmsFile(song.path);
	const tl = buildTimeline(parsed);
	const features = computeDifficultyFeatures(tl.notes, tl.durationSec);
	return {
		song,
		headers: parsed.headers,
		durationSec: tl.durationSec,
		is7key: tl.is7key,
		scratchCount: tl.scratchCount,
		lnCount: tl.lnCount,
		features,
	};
}

async function main(): Promise<void> {
	const dbPath = process.env.SONGDATA_DB_PATH ?? DEFAULT_SONGDATA_DB_PATH;
	const insanePath = process.env.INSANE_TABLE_PATH ?? DEFAULT_INSANE_TABLE_PATH;
	const normalPath = process.env.NORMAL_TABLE_PATH ?? DEFAULT_NORMAL_TABLE_PATH;
	const outPath = process.env.OUT_PATH ?? DEFAULT_OUTPUT_PATH;
	const anchorsPath = process.env.ANCHORS_PATH ?? DEFAULT_ANCHORS_PATH;
	const concurrency = clamp(parseIntSafe(process.env.CONCURRENCY ?? "4", 10) ?? 4, 1, 16);
	const debugSinglePath = process.env.DEBUG_SINGLE_PATH;
	const starMin = parseFloatSafe(process.env.STAR_MIN) ?? -12;
	const starMax = parseFloatSafe(process.env.STAR_MAX) ?? 40;
	const normalStarMin = parseFloatSafe(process.env.NORMAL_STAR_MIN) ?? -11;
	const normalStarMax = parseFloatSafe(process.env.NORMAL_STAR_MAX) ?? 2;
	const normalWeight = parseFloatSafe(process.env.NORMAL_WEIGHT) ?? 0.35;

	let missingFileLogs = 0;
	const maxMissingFileLogs = 20;

	if (!fs.existsSync(dbPath)) throw new Error(`songdata.db not found: ${dbPath}`);
	if (!fs.existsSync(insanePath)) throw new Error(`insane_data.json not found: ${insanePath}`);

	const songs = await readSongDataFromDb(dbPath);
	const insaneMd5ToLevel = readInsaneTableLevels(insanePath);
	const normalMd5ToRank = fs.existsSync(normalPath) ? readNormalTableLevels(normalPath) : new Map<string, number>();
	if (normalMd5ToRank.size > 0) {
		console.log(`normal table entries: ${normalMd5ToRank.size} (${normalPath})`);
	}

	if (debugSinglePath) {
		try {
			const fullPath = path.isAbsolute(debugSinglePath)
				? debugSinglePath
				: path.join(process.cwd(), debugSinglePath);
			const { features, durationSec } = analyzeBmsPathForTraining(fullPath);
			console.log("DEBUG_SINGLE_PATH features:", { path: fullPath, durationSec, features });
		} catch (e: any) {
			console.warn(`DEBUG_SINGLE_PATH failed: ${debugSinglePath} (${e?.message ?? e})`);
		}
	}

	// Add ★25 anchor: FREEDOM DiVE [FOUR DIMENSIONS]
	const fd = findFreedomDiveFourDimensions(songs);
	if (fd) insaneMd5ToLevel.set(fd.md5, 25);

	const songsByMd5 = new Map<string, SongRow>();
	for (const s of songs) songsByMd5.set(s.md5, s);

	console.log(`songs in DB: ${songs.length}`);

	// Pass 1) analyze only labeled charts to build calibration
	const trainingSongs: SongRow[] = [];
	for (const [md5] of insaneMd5ToLevel) {
		const s = songsByMd5.get(md5);
		if (s) trainingSongs.push(s);
	}
	console.log(`training charts found in DB: ${trainingSongs.length}`);

	const trainingSamples: Array<{ x: number[]; y: number; w: number; pred?: number }> = [];
	await mapLimit(trainingSongs, concurrency, async (song) => {
		const level = insaneMd5ToLevel.get(song.md5);
		if (!level) return;
		try {
			const r = await analyzeSong(song);
			const x = featureVector(r.features);
			const w = level === 25 ? 12 : 1;
			trainingSamples.push({ x, y: level, w });
		} catch (e: any) {
			if (e?.code === "ENOENT") {
				missingFileLogs++;
				if (missingFileLogs <= maxMissingFileLogs) {
					console.warn(`training analyze failed (ENOENT): ${song.path}`);
				} else if (missingFileLogs === maxMissingFileLogs + 1) {
					console.warn("training analyze failed (ENOENT): (more missing files suppressed)");
				}
				return;
			}
			const msg = e?.message ? String(e.message) : String(e);
			console.warn(`training analyze failed: ${song.path} (${msg})`);
		}
	});

	// Add normal table samples (low difficulty), skipping ones already in insane table.
	if (normalMd5ToRank.size > 0 && normalWeight > 0) {
		const normalSongs: SongRow[] = [];
		for (const [md5] of normalMd5ToRank) {
			if (insaneMd5ToLevel.has(md5)) continue;
			const s = songsByMd5.get(md5);
			if (!s) continue;
			normalSongs.push(s);
		}
		console.log(`normal charts found in DB: ${normalSongs.length}`);
		await mapLimit(normalSongs, concurrency, async (song) => {
			const rank = normalMd5ToRank.get(song.md5);
			if (!rank) return;
			try {
				const r = await analyzeSong(song);
				const x = featureVector(r.features);
				const y = normalRankToStar(rank, normalStarMin, normalStarMax);
				const w = normalWeight;
				trainingSamples.push({ x, y, w });
			} catch (e: any) {
				if (e?.code === "ENOENT") return;
				const msg = e?.message ? String(e.message) : String(e);
				console.warn(`normal training analyze failed: ${song.path} (${msg})`);
			}
		});
	}

	// Low-difficulty anchors (optional): allow ★0以下などのスケールを固定する。
	const anchors = readAnchors(anchorsPath);
	if (anchors.length > 0) {
		console.log(`anchors loaded: ${anchors.length} (${anchorsPath})`);
		for (const a of anchors) {
			try {
				const fullPath = path.isAbsolute(a.path) ? a.path : path.join(process.cwd(), a.path);
				const { features } = analyzeBmsPathForTraining(fullPath);
				const x = featureVector(features);
				const w = a.weight ?? 6;
				trainingSamples.push({ x, y: a.star, w });
			} catch (e: any) {
				console.warn(`anchor analyze failed: ${a.path} (${e?.message ?? e})`);
			}
		}
	}

	if (trainingSamples.length < 20) {
		throw new Error(`training samples too small: ${trainingSamples.length}`);
	}

	const ridge = trainRidgeRegression(trainingSamples, 2.0);
	for (const s of trainingSamples) s.pred = predictRidge(ridge, s.x);
	const calibrator = buildCalibrationMapper(
		trainingSamples.map((s) => ({ pred: s.pred ?? 0, truth: s.y, w: s.w }))
	);

	const mapper = (features: DifficultyFeatures) => {
		const pred = predictRidge(ridge, featureVector(features));
		return calibrator(pred);
	};

	// Pass 2) analyze candidates, filter, bucket (no need to keep all analysis results)
	const candidates = songs.filter(shouldAnalyzeByDbNotes);
	console.log(`candidates by DB notes range: ${candidates.length}`);

	const buckets = new Map<string, SongRow[]>();
	let passed = 0;
	await mapLimit(candidates, concurrency, async (song) => {
		try {
			const r = await analyzeSong(song);
			if (!passesFilters(song, r)) return;
			const star = clamp(mapper(r.features), starMin, starMax);
			let starRoundedNum = Math.round(star * 2) / 2;
			if (Object.is(starRoundedNum, -0)) starRoundedNum = 0;
			const starRounded = starRoundedNum.toFixed(1);
			const name = `★${starRounded}`;
			const arr = buckets.get(name) ?? [];
			arr.push(song);
			buckets.set(name, arr);
			passed++;
		} catch (e: any) {
			if (e?.code === "ENOENT") {
				missingFileLogs++;
				if (missingFileLogs <= maxMissingFileLogs) {
					console.warn(`analyze failed (ENOENT): ${song.path}`);
				} else if (missingFileLogs === maxMissingFileLogs + 1) {
					console.warn("analyze failed (ENOENT): (more missing files suppressed)");
				}
				return;
			}
			const msg = e?.message ? String(e.message) : String(e);
			console.warn(`analyze failed: ${song.path} (${msg})`);
		}
	});

	const folder = [...buckets.entries()]
		.map(([name, songs]) => {
			songs.sort((a, b) => (a.title ?? "").localeCompare(b.title ?? "", "ja"));
			return {
				class: "bms.player.beatoraja.TableData$TableFolder",
				name,
				songs: songs.map(buildSongDataObject),
			};
		})
		.sort((a, b) => {
			const na = parseFloat(String(a.name).replace("★", ""));
			const nb = parseFloat(String(b.name).replace("★", ""));
			return (Number.isFinite(na) ? na : 0) - (Number.isFinite(nb) ? nb : 0);
		});

	const output = { folder };
	fs.writeFileSync(outPath, JSON.stringify(output, null, 4), "utf8");

	console.log(`passed filters: ${passed}`);
	console.log(`folders written: ${folder.length}`);
	console.log(`output: ${outPath}`);

	// Print a small training summary
	const byLevel = new Map<number, number[]>();
	for (const s of trainingSamples) {
		const arr = byLevel.get(s.y) ?? [];
		arr.push(s.pred ?? 0);
		byLevel.set(s.y, arr);
	}
	const levels = [...byLevel.keys()].sort((a, b) => a - b);
	const summary = levels.map((l) => ({ level: l, n: (byLevel.get(l) ?? []).length, predMedian: median(byLevel.get(l) ?? []) }));
	console.log("training pred medians:", summary);
}

main().catch((e) => {
	console.error(e);
	process.exitCode = 1;
});
