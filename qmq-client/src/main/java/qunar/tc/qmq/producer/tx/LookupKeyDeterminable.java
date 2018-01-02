package qunar.tc.qmq.producer.tx;

/**
 * User: zhaohuiyu
 * Date: 8/4/14
 * Time: 2:21 PM
 *
 * 使用spring的routing datasource的时候，如果routing datasource里面的数据源是非原生数据源(比如PXC)
 * 则建议实现下面的逻辑
 * class DynamicDataSource extends AbstractRoutingDataSource implements LookupKeyDeterminable{
 *     public String currentKey(){
 *         //当前的写库
 *         DataSource currentWriter = determineTargetDataSource();
 *         return JdbcUtils.getDatabaseInstanceURL(currentWriter);
 *     }
 * }
 */
public interface LookupKeyDeterminable {
    String currentKey();
}
