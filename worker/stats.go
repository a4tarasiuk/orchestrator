package worker

import (
	"log"

	"github.com/c9s/goprocinfo/linux"
)

type Stats struct {
	Memory *linux.MemInfo

	Disk *linux.Disk

	CPU *linux.CPUStat

	Load *linux.LoadAvg

	TaskCount int
}

func GetStats() *Stats {
	return &Stats{
		Memory: GetMemoryInfo(),
		Disk:   GetDiskInfo(),
		CPU:    GetCPUStats(),
		Load:   GetLoadAvg(),
	}
}

func (s *Stats) GetTotalMemoryKB() uint64 {
	return s.Memory.MemTotal
}

func (s *Stats) GetAvailableMemoryKB() uint64 {
	return s.Memory.MemAvailable
}

func (s *Stats) GetUsedMemoryKB() uint64 {
	return s.GetTotalMemoryKB() - s.GetAvailableMemoryKB()
}

func (s *Stats) GetUsedMemoryPercentage() uint64 {
	return s.GetAvailableMemoryKB() / s.GetTotalMemoryKB()
}

func (s *Stats) GetDiskTotal() uint64 {
	return s.Disk.All
}

func (s *Stats) GetDiskFree() uint64 {
	return s.Disk.Free
}

func (s *Stats) GetDiskUsed() uint64 {
	return s.Disk.Used
}

func (s *Stats) GetCPUUsage() float64 {
	totalIdle := s.CPU.Idle + s.CPU.IOWait

	totalNonIdle := s.CPU.User + s.CPU.Nice + s.CPU.System + s.CPU.IRQ + s.CPU.SoftIRQ + s.CPU.Steal

	total := totalIdle + totalNonIdle

	if total == 0 {
		return 0.00
	}

	return (float64(total) - float64(totalIdle)) / float64(total)
}

func GetMemoryInfo() *linux.MemInfo {
	memstats, err := linux.ReadMemInfo("/proc/meminfo")
	if err != nil {
		log.Printf("Error reading from /proc/meminfo")
		return &linux.MemInfo{}
	}
	return memstats
}

// GetDiskInfo See https://godoc.org/github.com/c9s/goprocinfo/linux#Disk
func GetDiskInfo() *linux.Disk {
	diskstats, err := linux.ReadDisk("/")
	if err != nil {
		log.Printf("Error reading from /")
		return &linux.Disk{}
	}
	return diskstats
}

// GetCPUStats See https://godoc.org/github.com/c9s/goprocinfo/linux#CPUStat
func GetCPUStats() *linux.CPUStat {
	stats, err := linux.ReadStat("/proc/stat")
	if err != nil {
		log.Printf("Error reading from /proc/stat")
		return &linux.CPUStat{}
	}
	return &stats.CPUStatAll
}

// GetLoadAvg See https://godoc.org/github.com/c9s/goprocinfo/linux#LoadAvg
func GetLoadAvg() *linux.LoadAvg {
	loadavg, err := linux.ReadLoadAvg("/proc/loadavg")
	if err != nil {
		log.Printf("Error reading from /proc/loadavg")
		return &linux.LoadAvg{}
	}
	return loadavg
}
