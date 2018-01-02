package qunar.tc.qmq.meta.store;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.base.BrokerGroup;
import qunar.tc.qmq.base.BrokerState;
import qunar.tc.qmq.meta.model.SubjectRoute;
import qunar.tc.qmq.serializer.Serializer;
import qunar.tc.qmq.serializer.SerializerFactory;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;

import static qunar.tc.qmq.meta.store.DatabaseUtils.createDataSource;

/**
 * @author yunfeng.yang
 * @since 2017/8/31
 */
public class DatabaseStore implements Store {
    private static final Serializer SERIALIZER = SerializerFactory.create();

    private static final String INSERT_SUBJECT_ROUTE_SQL = "INSERT IGNORE INTO subject_route(subject_info, broker_group_json, create_time) VALUES(?, ?, ?)";
    private static final String FIND_SUBJECT_ROUTE_SQL = "SELECT subject_info, broker_group_json, update_time FROM subject_route";
    private static final String SELECT_SUBJECT_ROUTE_SQL = "SELECT broker_group_json FROM subject_route WHERE subject_info = ?";

    private static final String FIND_BROKER_GROUP_SQL = "SELECT group_name, master_address, broker_state, update_time FROM broker_group";
    private static final String SELECT_BROKER_GROUP_SQL = "SELECT group_name, master_address, broker_state, update_time FROM broker_group WHERE group_name = ?";
    private static final String UPDATE_BROKER_GROUP_SQL = "UPDATE broker_group SET broker_state = ? WHERE group_name = ?";
    private static final String INSERT_OR_UPDATE_BROKER_GROUP_SQL =
            "INSERT INTO broker_group(group_name, master_address, broker_state, create_time) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE group_name = ?, master_address = ?, broker_state = ?";

    private final JdbcTemplate jdbcTemplate;

    public DatabaseStore() {
        final DataSource dataSource = createDataSource("datasource.properties");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private static final RowMapper<BrokerGroup> BROKER_GROUP_ROW_MAPPER = (rs, rowNum) -> {
        final BrokerGroup brokerGroup = new BrokerGroup();
        brokerGroup.setGroupName(rs.getString("group_name"));
        brokerGroup.setMaster(rs.getString("master_address"));
        brokerGroup.setBrokerState(BrokerState.codeOf(rs.getInt("broker_state")));
        brokerGroup.setUpdateTime(rs.getTimestamp("update_time").getTime());

        return brokerGroup;
    };

    private static final RowMapper<SubjectRoute> SUBJECT_ROUTE_ROW_MAPPER = (rs, rowNum) -> {
        final String subject = rs.getString("subject_info");
        final String groupInfoJson = rs.getString("broker_group_json");
        final Timestamp updateTime = rs.getTimestamp("update_time");
        final List<String> groupNames = SERIALIZER.deSerialize(groupInfoJson, new TypeReference<List<String>>() {
        });
        final SubjectRoute subjectRoute = new SubjectRoute();
        subjectRoute.setSubject(subject);
        subjectRoute.setBrokerGroups(groupNames);
        subjectRoute.setUpdateTime(updateTime.getTime());
        return subjectRoute;
    };

    @Override
    public int insertSubjectRoute(String subject, List<String> groupNames) {
        final String serialize = SERIALIZER.serialize(groupNames);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        return jdbcTemplate.update(INSERT_SUBJECT_ROUTE_SQL, subject, serialize, now);
    }

    @Override
    public SubjectRoute selectSubjectRoute(String subject) {
        return jdbcTemplate.queryForObject(SELECT_SUBJECT_ROUTE_SQL, SUBJECT_ROUTE_ROW_MAPPER, subject);
    }

    @Override
    public void insertOrUpdateBrokerGroup(final String groupName, final String masterAddress, final BrokerState brokerState) {
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(INSERT_OR_UPDATE_BROKER_GROUP_SQL, groupName, masterAddress, brokerState.getCode(), now, groupName, masterAddress, brokerState.getCode());
    }

    @Override
    public void updateBrokerGroup(String groupName, BrokerState brokerState) {
        jdbcTemplate.update(UPDATE_BROKER_GROUP_SQL, brokerState.getCode(), groupName);
    }

    @Override
    public List<BrokerGroup> getAllBrokerGroups() {
        return jdbcTemplate.query(FIND_BROKER_GROUP_SQL, BROKER_GROUP_ROW_MAPPER);
    }

    @Override
    public List<SubjectRoute> getAllSubjectRoutes() {
        return jdbcTemplate.query(FIND_SUBJECT_ROUTE_SQL, SUBJECT_ROUTE_ROW_MAPPER);
    }

    @Override
    public BrokerGroup getBrokerGroup(String groupName) {
        return jdbcTemplate.queryForObject(SELECT_BROKER_GROUP_SQL, BROKER_GROUP_ROW_MAPPER, groupName);
    }
}
