variable "project_id" {
    type = string
}
variable "region" {
    type = string
    default = "asia-southeast1"
}
variable "zone" {
  type = string
  default = "asia-southeast1-a"
}
variable "cluster_name" {
    type = string
    default = "datawarehouse"
  
}
variable "enable_gke" {
    default = true
  
}

variable "enable_composer" {
    default = true
  
}
variable "vpc_cidr" {
  default = "10.10.0.0/24"
}
