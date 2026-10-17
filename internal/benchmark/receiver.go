package benchmark

import (
	"context"
	"fmt"
	"net"
)

func Receive(ctx context.Context, listenAddress string, readBuffer int) error {
	address, err := net.ResolveUDPAddr("udp4", listenAddress)
	if err != nil {
		return fmt.Errorf("resolve listen address: %w", err)
	}
	connection, err := net.ListenUDP("udp4", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer connection.Close()
	if readBuffer > 0 {
		if err := connection.SetReadBuffer(readBuffer); err != nil {
			return fmt.Errorf("set UDP read buffer: %w", err)
		}
	}
	go func() {
		<-ctx.Done()
		_ = connection.Close()
	}()
	buffer := make([]byte, 65535)
	for {
		if _, _, err := connection.ReadFromUDP(buffer); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive UDP packet: %w", err)
		}
	}
}
