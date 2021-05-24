## Startup Druid And Debug

### XML App Def
You can configure application definitions in XML for import into IntelliJ. 
File placed in `dev/debug/intellij-app-xml` are a few examples. These should be copied to .idea/runConfigurations 
in the Druid source code. 

### Startup ZooKeeper
This also assumes you have [ZooKeeper](http://zookeeper.apache.org/releases.html) running locally, 
which usually just involves downloading the latst distribution of ZooKeeper, 
doing some minor configuration in ZooKeeper's `conf/` directory (most defaults are fine), 
then running `./bin/zkServer.sh start` in the ZooKeeper directory. On macOS, you can also 
achieve this through the following commands

1. `brew install zookeeper`
2. `brew services start zookeeper` or `zkServer start`

### Initial Build
Before running or debugging the apps, you should do a `mvn clean install -Pdist -DskipTests` 
in the Druid source in order to make sure directories are populated correctly.
`-Pdist` is required because it puts all core extensions under `distribution\target\extensions` directory, 
where `runConfigurations` below could load extensions from.
You may also add `-Ddruid.console.skip=true` to the command if you're focusing on backend servers 
instead of frontend project. This option saves great building time.

### Startup Druid Component App
App configuration properties file are placed in `dev/debug/conf`, startup CoordinatorOverload, Historical, Broker, MiddleManager, Router.

### Reference Doc
* [intellij-setup](https://github.com/apache/druid/blob/master/dev/intellij-setup.md)