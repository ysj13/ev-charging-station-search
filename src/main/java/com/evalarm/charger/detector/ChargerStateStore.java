package com.evalarm.charger.detector;

import com.evalarm.charger.station.domain.ChargerKey;
import com.evalarm.charger.station.domain.ChargerStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * 충전기별 직전 상태를 Redis에 보관한다.
 * 키: charger:state:{stationId}:{chargerId}, 값: "{status}|{statusUpdatedAt epochMilli}"
 *
 * <p>쓰기는 Lua 스크립트로 "저장된 값보다 최신일 때만" 원자적으로 덮어쓴다.
 * 컨슈머 재조정 등으로 같은 충전기를 두 곳에서 잠시 처리하더라도 상태가 과거로 되돌아가지 않는다.
 */
@Component
public class ChargerStateStore {

    private static final String KEY_PREFIX = "charger:state:";
    private static final int CHUNK_SIZE = 1_000;

    private static final DefaultRedisScript<Long> SAVE_IF_NEWER = new DefaultRedisScript<>(
            """
            for i, key in ipairs(KEYS) do
              local incoming = ARGV[i]
              local incomingTs = tonumber(string.sub(incoming, string.find(incoming, '|', 1, true) + 1))
              local current = redis.call('GET', key)
              local write = true
              if current then
                local sep = string.find(current, '|', 1, true)
                if sep then
                  local currentTs = tonumber(string.sub(current, sep + 1))
                  if currentTs and currentTs >= incomingTs then write = false end
                end
              end
              if write then redis.call('SET', key, incoming) end
            end
            return #KEYS""",
            long.class);

    private final StringRedisTemplate redisTemplate;

    public ChargerStateStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Map<ChargerKey, ChargerState> load(Collection<ChargerKey> keys) {
        if (keys.isEmpty()) {
            return Map.of();
        }
        Map<ChargerKey, ChargerState> result = new HashMap<>(keys.size());
        for (List<ChargerKey> chunk : chunked(keys)) {
            List<String> redisKeys = new ArrayList<>(chunk.size());
            for (ChargerKey key : chunk) {
                redisKeys.add(redisKey(key));
            }
            List<String> values = redisTemplate.opsForValue().multiGet(redisKeys);
            if (values == null) {
                values = List.of();
            }
            int size = Math.min(chunk.size(), values.size());
            for (int i = 0; i < size; i++) {
                ChargerState state = decode(values.get(i));
                if (state != null) {
                    result.put(chunk.get(i), state);
                }
            }
        }
        return result;
    }

    public void saveIfNewer(Map<ChargerKey, ChargerState> states) {
        for (List<Map.Entry<ChargerKey, ChargerState>> chunk : chunked(states.entrySet())) {
            List<String> redisKeys = new ArrayList<>(chunk.size());
            Object[] args = new Object[chunk.size()];
            for (int i = 0; i < chunk.size(); i++) {
                redisKeys.add(redisKey(chunk.get(i).getKey()));
                args[i] = encode(chunk.get(i).getValue());
            }
            redisTemplate.execute(SAVE_IF_NEWER, redisKeys, args);
        }
    }

    private static String redisKey(ChargerKey key) {
        return KEY_PREFIX + key.stationId() + ":" + key.chargerId();
    }

    private static String encode(ChargerState state) {
        return state.status().name() + "|" + state.statusUpdatedAt().toEpochMilli();
    }

    private static @Nullable ChargerState decode(@Nullable String value) {
        if (value == null) {
            return null;
        }
        int separator = value.indexOf('|');
        if (separator < 0) {
            return null;
        }
        ChargerStatus status;
        try {
            status = ChargerStatus.valueOf(value.substring(0, separator));
        } catch (RuntimeException e) {
            return null;
        }
        long epochMilli;
        try {
            epochMilli = Long.parseLong(value.substring(separator + 1));
        } catch (NumberFormatException e) {
            return null;
        }
        return new ChargerState(status, Instant.ofEpochMilli(epochMilli));
    }

    /** 순서를 유지한 채 CHUNK_SIZE개씩 나눈다 */
    private static <T> List<List<T>> chunked(Collection<T> items) {
        List<T> all = new ArrayList<>(items);
        List<List<T>> chunks = new ArrayList<>((all.size() + CHUNK_SIZE - 1) / CHUNK_SIZE);
        for (int from = 0; from < all.size(); from += CHUNK_SIZE) {
            chunks.add(all.subList(from, Math.min(from + CHUNK_SIZE, all.size())));
        }
        return chunks;
    }
}
