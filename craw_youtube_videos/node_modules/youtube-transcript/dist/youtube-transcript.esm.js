//#region src/index.ts
var e = /(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([^"&?\/\s]{11})/i, t = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36,gzip(gfe)", n = /<text start="([^"]*)" dur="([^"]*)">([^<]*)<\/text>/g, r = "https://www.youtube.com/youtubei/v1/player?prettyPrint=false", i = "20.10.38", a = { client: {
	clientName: "ANDROID",
	clientVersion: i
} }, o = `com.google.android.youtube/${i} (Linux; U; Android 14)`, s = class extends Error {
	constructor(e) {
		super(`[YoutubeTranscript] 🚨 ${e}`);
	}
}, c = class extends s {
	constructor() {
		super("YouTube is receiving too many requests from this IP and now requires solving a captcha to continue");
	}
}, l = class extends s {
	constructor(e) {
		super(`The video is no longer available (${e})`);
	}
}, u = class extends s {
	constructor(e) {
		super(`Transcript is disabled on this video (${e})`);
	}
}, d = class extends s {
	constructor(e) {
		super(`No transcripts are available for this video (${e})`);
	}
}, f = class extends s {
	constructor(e, t, n) {
		super(`No transcripts are available in ${e} this video (${n}). Available languages: ${t.join(", ")}`);
	}
}, p = class {
	static async fetchTranscript(e, t) {
		let n = this.retrieveVideoId(e);
		return await this.fetchViaInnerTube(n, t) || this.fetchViaWebPage(n, e, t);
	}
	static async fetchViaInnerTube(e, t) {
		try {
			let n = await (t?.fetch ?? fetch)(r, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					"User-Agent": o
				},
				body: JSON.stringify({
					context: a,
					videoId: e
				})
			});
			if (!n.ok) return;
			let i = (await n.json())?.captions?.playerCaptionsTracklistRenderer?.captionTracks;
			return !Array.isArray(i) || i.length === 0 ? void 0 : this.fetchTranscriptFromTracks(i, e, t);
		} catch {
			return;
		}
	}
	static async fetchViaWebPage(e, n, r) {
		let i = await (await (r?.fetch ?? fetch)(`https://www.youtube.com/watch?v=${e}`, { headers: {
			...r?.lang && { "Accept-Language": r.lang },
			"User-Agent": t
		} })).text();
		if (i.includes("class=\"g-recaptcha\"")) throw new c();
		if (!i.includes("\"playabilityStatus\":")) throw new l(n);
		let a = this.parseInlineJson(i, "ytInitialPlayerResponse")?.captions?.playerCaptionsTracklistRenderer?.captionTracks;
		if (!Array.isArray(a) || a.length === 0) throw new u(n);
		return this.fetchTranscriptFromTracks(a, n, r);
	}
	static parseInlineJson(e, t) {
		let n = `var ${t} = `, r = e.indexOf(n);
		if (r === -1) return null;
		let i = r + n.length, a = 0;
		for (let t = i; t < e.length; t++) if (e[t] === "{") a++;
		else if (e[t] === "}" && (a--, a === 0)) try {
			return JSON.parse(e.slice(i, t + 1));
		} catch {
			return null;
		}
		return null;
	}
	static async fetchTranscriptFromTracks(e, n, r) {
		if (r?.lang && !e.some((e) => e.languageCode === r?.lang)) throw new f(r?.lang, e.map((e) => e.languageCode), n);
		let i = (r?.lang ? e.find((e) => e.languageCode === r?.lang) : e[0]).baseUrl;
		try {
			if (!new URL(i).hostname.endsWith(".youtube.com")) throw new d(n);
		} catch (e) {
			throw e instanceof s ? e : new d(n);
		}
		let a = await (r?.fetch ?? fetch)(i, { headers: {
			...r?.lang && { "Accept-Language": r.lang },
			"User-Agent": t
		} });
		if (!a.ok) throw new d(n);
		let o = await a.text(), c = r?.lang ?? e[0].languageCode;
		return this.parseTranscriptXml(o, c);
	}
	static parseTranscriptXml(e, t) {
		let r = [], i = /<p\s+t="(\d+)"\s+d="(\d+)"[^>]*>([\s\S]*?)<\/p>/g, a;
		for (; (a = i.exec(e)) !== null;) {
			let e = parseInt(a[1], 10), n = parseInt(a[2], 10), i = a[3], o = "", s = /<s[^>]*>([^<]*)<\/s>/g, c;
			for (; (c = s.exec(i)) !== null;) o += c[1];
			o ||= i.replace(/<[^>]+>/g, ""), o = this.decodeEntities(o).trim(), o && r.push({
				text: o,
				duration: n,
				offset: e,
				lang: t
			});
		}
		return r.length > 0 ? r : [...e.matchAll(n)].map((e) => ({
			text: this.decodeEntities(e[3]),
			duration: parseFloat(e[2]),
			offset: parseFloat(e[1]),
			lang: t
		}));
	}
	static decodeEntities(e) {
		return e.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, "\"").replace(/&#39;/g, "'").replace(/&apos;/g, "'").replace(/&#x([0-9a-fA-F]+);/g, (e, t) => String.fromCodePoint(parseInt(t, 16))).replace(/&#(\d+);/g, (e, t) => String.fromCodePoint(parseInt(t, 10)));
	}
	static retrieveVideoId(t) {
		if (t.length === 11) return t;
		let n = t.match(e);
		if (n && n.length) return n[1];
		throw new s("Impossible to retrieve Youtube video ID.");
	}
};
function m(e, t) {
	return p.fetchTranscript(e, t);
}
//#endregion
export { p as YoutubeTranscript, u as YoutubeTranscriptDisabledError, s as YoutubeTranscriptError, d as YoutubeTranscriptNotAvailableError, f as YoutubeTranscriptNotAvailableLanguageError, c as YoutubeTranscriptTooManyRequestError, l as YoutubeTranscriptVideoUnavailableError, m as fetchTranscript };
