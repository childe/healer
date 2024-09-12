# INSTALL

```sh
go install github.com/childe/healer/command/healer@latest
```

# Docker

```sh
docker pull rmself/healer:latest
```

# what can healer command do

- produce messages
- consume messages
- get metadata
- create topics
- delete topics
- create(increase) partitons
- describe configs
- alter configs
- alter partiton assignments
- get offsets
- get pendings
- reset offsets
- (re)elect leaders
- rest apis of doing jobs above

# Code Examples

## Group Consumer

group consumer(cluster style)

[https://github.com/childe/healer/blob/master/command/healer/cmd/group-consumer.go](https://github.com/childe/healer/blob/master/command/healer/cmd/group-consumer.go)


## Producer

[https://github.com/childe/healer/blob/master/command/healer/cmd/console-producer.go](https://github.com/childe/healer/blob/master/command/healer/cmd/console-producer.go)


## Console Consumer

one consumer consume messages from all partitons

[https://github.com/childe/healer/blob/master/command/healer/cmd/console-consumer.go](https://github.com/childe/healer/blob/master/command/healer/cmd/console-consumer.go)


## Simple Consumer

consume from only one certain partition

[https://github.com/childe/healer/blob/master/command/healer/cmd/simple-consumer.go](https://github.com/childe/healer/blob/master/command/healer/cmd/simple-consumer.go)
