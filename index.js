"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
exports.__esModule = true;
var bullmq_1 = require("bullmq");
var redis = require("ioredis");
var needle = require("needle");
var chunk = require("chunk");
var p_ratelimit_1 = require("p-ratelimit");
var pipe = require("p-pipe");
var redisClient = new redis('ec2-3-236-123-111.compute-1.amazonaws.com', { enableAutoPipelining: true });
var queueSchedulers = [new bullmq_1.QueueScheduler('import'), new bullmq_1.QueueScheduler('follower_import'), new bullmq_1.QueueScheduler('export')];
var queueOptions = { defaultJobOptions: { removeOnComplete: true } };
var importQueue = new bullmq_1.Queue('import', queueOptions);
var followerImportQueue = new bullmq_1.Queue('follower_import', queueOptions);
var exportQueue = new bullmq_1.Queue('export', queueOptions); //QUEUE OF MARSHALLED
var getTwitterUsersLimit = p_ratelimit_1.pRateLimit({
    interval: 15 * 60 * 1000,
    rate: 500
});
var getTwitterFollowersLimit = p_ratelimit_1.pRateLimit({
    interval: 15 * 60 * 1000,
    rate: 20
});
function getTwitterUsers(userIds) {
    return __awaiter(this, void 0, void 0, function () {
        var twitterUserResponse, twitterUsers;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, getTwitterUsersLimit(function () { return needle('get', "https://api.twitter.com/2/users?ids=" + userIds.join(',') + "&user.fields=profile_image_url,public_metrics", {
                        headers: {
                            'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAABCINwEAAAAAE84d%2BfYIClOTvkrWajggz6%2FnQEo%3DCFjvHp6J0wnPIQSCA0IF9RLr0aPI4O7MkevqKsiawqJihElwmB'
                        }
                    }); })];
                case 1:
                    twitterUserResponse = _a.sent();
                    if (twitterUserResponse.body.errors)
                        throw Error(twitterUserResponse.body.errors);
                    twitterUsers = twitterUserResponse.body.data;
                    return [2 /*return*/, twitterUsers];
            }
        });
    });
}
function getTwitterFollowers(userid) {
    return __awaiter(this, void 0, void 0, function () {
        var twitterUserResponse, twitterUsers;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, getTwitterFollowersLimit(function () { return needle('get', "https://api.twitter.com/2/users/" + userid + "/following?user.fields=profile_image_url,created_at,public_metrics&max_results=1000", {
                        headers: {
                            'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAABCINwEAAAAAE84d%2BfYIClOTvkrWajggz6%2FnQEo%3DCFjvHp6J0wnPIQSCA0IF9RLr0aPI4O7MkevqKsiawqJihElwmB'
                        }
                    }); })];
                case 1:
                    twitterUserResponse = _a.sent();
                    if (!twitterUserResponse.body.data)
                        throw Error(twitterUserResponse.body);
                    twitterUsers = twitterUserResponse.body.data;
                    return [2 /*return*/, twitterUsers];
            }
        });
    });
}
function addToRedisSet(marshalledUsersInfo, userIds) {
    if (!userIds.length)
        return;
    return Promise.all([
        redisClient.sadd('twitterIds', userIds),
        redisClient.sadd('twitterusers', marshalledUsersInfo)
    ]);
}
function removeExistingUsers(twitterUsers) {
    return __awaiter(this, void 0, void 0, function () {
        var userids, result;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    userids = twitterUsers.map(function (t) { return t.id; });
                    return [4 /*yield*/, redisClient.smismember('twitterIds', userids)];
                case 1:
                    result = _a.sent();
                    return [2 /*return*/, twitterUsers.filter(function (_, i) { return result[i] === 0; })];
            }
        });
    });
}
function addFollowerImportJob(id) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, followerImportQueue.add('follower_import', id, { attempts: 20, backoff: { type: 'exponential', delay: 1000 } })];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function addUserImportJob(userids) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, importQueue.add('import', userids, { attempts: 20, backoff: { type: 'exponential', delay: 1000 } })];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function addUserInfoExportJob(usersInfo) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, exportQueue.add('export', usersInfo, { attempts: 500, backoff: { type: 'linear', delay: 10 } })];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function addInitialJob() {
    var e_1, _a;
    return __awaiter(this, void 0, void 0, function () {
        var jobs, jobs_1, jobs_1_1, job, state, e_1_1;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, exportQueue.getJobs(["active", "waiting", "delayed", "complete", "completed"])];
                case 1:
                    jobs = _b.sent();
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, 8, 9, 14]);
                    jobs_1 = __asyncValues(jobs);
                    _b.label = 3;
                case 3: return [4 /*yield*/, jobs_1.next()];
                case 4:
                    if (!(jobs_1_1 = _b.sent(), !jobs_1_1.done)) return [3 /*break*/, 7];
                    job = jobs_1_1.value;
                    return [4 /*yield*/, job.getState()];
                case 5:
                    state = _b.sent();
                    console.log(state);
                    _b.label = 6;
                case 6: return [3 /*break*/, 3];
                case 7: return [3 /*break*/, 14];
                case 8:
                    e_1_1 = _b.sent();
                    e_1 = { error: e_1_1 };
                    return [3 /*break*/, 14];
                case 9:
                    _b.trys.push([9, , 12, 13]);
                    if (!(jobs_1_1 && !jobs_1_1.done && (_a = jobs_1["return"]))) return [3 /*break*/, 11];
                    return [4 /*yield*/, _a.call(jobs_1)];
                case 10:
                    _b.sent();
                    _b.label = 11;
                case 11: return [3 /*break*/, 13];
                case 12:
                    if (e_1) throw e_1.error;
                    return [7 /*endfinally*/];
                case 13: return [7 /*endfinally*/];
                case 14: return [2 /*return*/];
            }
        });
    });
}
var createUserImportJobs = function (validAccounts) {
    var userIds = validAccounts.map(function (a) { return a.id; });
    chunk(userIds, 100).forEach(addUserImportJob);
    return validAccounts;
};
var createFollowerImportJobs = function (validAccounts) { return validAccounts.map(function (validAccount) {
    addFollowerImportJob(validAccount.id);
    return validAccount;
}); };
var followerImportWorker = new bullmq_1.Worker('follower_import', function (job) { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, followerImportPipeline(job.data)];
            case 1:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); }, { concurrency: 15 });
var followerImportPipeline = pipe(getTwitterFollowers, removeInvalidAccounts, removeExistingUsers, createUserImportJobs, addUserInfoExportJob);
followerImportWorker.on('completed', function (job) {
    console.log("(follower-import) done:" + job.id);
});
followerImportWorker.on('failed', function (job) {
    console.error("(follower-import) failed: " + job.id + " " + job.data + " " + JSON.stringify(job.failedReason));
});
var importPipeline = pipe(getTwitterUsers, createFollowerImportJobs, removeInvalidAccounts, addUserInfoExportJob);
var importWorker = new bullmq_1.Worker('import', function (job) { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, importPipeline(job.data)];
            case 1:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); }, { concurrency: 300 });
importWorker.on('completed', function (job) {
    console.log("(import) done: " + job.id);
});
importWorker.on('failed', function (job) {
    console.error("(import) failed: " + job.id + " " + JSON.stringify(job.data) + " " + JSON.stringify(job.failedReason));
});
var exportWorker = new bullmq_1.Worker('export', function (job) { return __awaiter(void 0, void 0, void 0, function () {
    var marshalledUsersInfo, userIds, error_1;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                _a.trys.push([0, 2, , 3]);
                marshalledUsersInfo = job.data.map(marshallUserInfo);
                userIds = job.data.map(function (d) { return d.id; });
                return [4 /*yield*/, addToRedisSet(marshalledUsersInfo, userIds)];
            case 1:
                _a.sent();
                return [3 /*break*/, 3];
            case 2:
                error_1 = _a.sent();
                console.error(error_1);
                if (error_1.message.includes('photo')) {
                    return [2 /*return*/];
                }
                throw error_1;
            case 3: return [2 /*return*/];
        }
    });
}); }, { concurrency: 1000 });
exportWorker.on('completed', function (job) {
    console.log("(export) done:" + job.id);
});
exportWorker.on('failed', function (_a) {
    var data = _a.data, job = __rest(_a, ["data"]);
    console.error("(export) failed: " + job.reasonFailed + " " + JSON.stringify(data));
});
function removeInvalidAccounts(usersInfo) {
    return usersInfo
        .filter(function (_a) {
        var _b = _a.public_metrics, followers_count = _b.followers_count, following_count = _b.following_count, tweet_count = _b.tweet_count, profile_image_url = _a.profile_image_url;
        return following_count >= 100 && followers_count > 5 && tweet_count > 3 && profile_image_url !== 'https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png';
    });
}
function marshallUserInfo(_a) {
    var username = _a.username, photoUrl = _a.profile_image_url, followers_count = _a.public_metrics.followers_count;
    return [username, parsePhotoIdFromPhotoUrl(photoUrl), followers_count].join('\n');
}
function parsePhotoIdFromPhotoUrl(photoUrl) {
    try {
        var regex = /(?:s\/)(.*)(?:_)/;
        return regex.exec(photoUrl)[1];
    }
    catch (error) {
        throw Error('Could not get photo id from url:' + photoUrl);
    }
}
addInitialJob()
    .then(function () { queueSchedulers.forEach(function (s) { return s.close(); }); })["catch"](console.error);
// https://abs.twimg.com/sticky/default_profile_images/default_profile_400x400.png
