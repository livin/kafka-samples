import com.airlivin.learn.kafka.wikimedia.sse.ServerSentEvent;
import com.airlivin.learn.kafka.wikimedia.sse.mappers.SSEStreamMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSEStreamMapperTest {
    private static final String SAMPLE_EVENT_LOG =
"""
event: message
id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":1701981000001}]
data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://es.wikipedia.org/wiki/Willow_Smith","request_id":"e8af5168-22a4-4beb-8b53-b3d1f5e8596f","id":"551f5fd1-7d81-4a33-be1a-7bd2d0d45853","dt":"2023-12-07T20:30:00Z","domain":"es.wikipedia.org","stream":"mediawiki.recentchange","topic":"codfw.mediawiki.recentchange","partition":0,"offset":825114112},"id":300811763,"type":"edit","namespace":0,"title":"Willow Smith","title_url":"https://es.wikipedia.org/wiki/Willow_Smith","comment":"","timestamp":1701981000,"user":"J0cj0cj0c","bot":false,"notify_url":"https://es.wikipedia.org/w/index.php?diff=155877510&oldid=155837516","minor":false,"length":{"old":16103,"new":16121},"revision":{"old":155837516,"new":155877510},"server_url":"https://es.wikipedia.org","server_name":"es.wikipedia.org","server_script_path":"/w","wiki":"eswiki","parsedcomment":""}

event: message
id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":1701981000001}]
data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://en.wikipedia.org/wiki/User:Kalshey","request_id":"fe8d0c8f-1c8b-4477-90a0-dddfaab12a62","id":"0c2f0466-2c31-44d4-afe3-ba76365aa382","dt":"2023-12-07T20:30:00Z","domain":"en.wikipedia.org","stream":"mediawiki.recentchange","topic":"codfw.mediawiki.recentchange","partition":0,"offset":825114113},"id":1701994024,"type":"log","namespace":2,"title":"User:Kalshey","title_url":"https://en.wikipedia.org/wiki/User:Kalshey","comment":"","timestamp":1701981000,"user":"Kalshey","bot":false,"log_id":156288747,"log_type":"newusers","log_action":"create","log_params":{"userid":47019178},"log_action_comment":"New user account","server_url":"https://en.wikipedia.org","server_name":"en.wikipedia.org","server_script_path":"/w","wiki":"enwiki","parsedcomment":""}
""";

    private SSEStreamMapper mapper;


    @BeforeEach
    void setUp() {
        mapper = new SSEStreamMapper();
    }

    @Test
    void testIt() {
        var events = mapper.toEvents(getEventStreamLines()).toList();
        assertEquals(2, events.size());

        ServerSentEvent event1 = events.get(0);
        assertEquals("message", event1.event());
        assertTrue(event1.id().contains("1701981000001"));
        assertTrue(event1.data().contains("e8af5168-22a4-4beb-8b53-b3d1f5e8596f"));
    }

    private static Stream<String> getEventStreamLines() {
        return Arrays.stream(SAMPLE_EVENT_LOG.split("\n"));
    }
}
