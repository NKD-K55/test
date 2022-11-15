<?php
defined('BASEPATH') OR exit('No direct script access allowed');

class CartController extends CI_Controller {
    function __construct() {
        parent::__construct();
        $this->load->model('frontend/Morder');
        $this->load->model('frontend/Morderdetail');
        $this->load->model('frontend/Mcustomer');
        $this->load->model('frontend/Mcategory');
        $this->load->model('frontend/Mconfig');
        $this->load->model('frontend/Mdistrict');
        $this->load->model('frontend/Mprovince');
        $this->load->model('frontend/MCart');

        $this->load->model('backend/Mproduct');

        $this->load->library('session');

        $this->data['com']='cart';
    }

    public function addProduct($id) {
        try {
            $sanPham = $this->Mproduct->product_detail($id);
            if ($sanPham != null) {
                if($sanPham['number']-$sanPham['number_buy'] < 1) {
                    $response = ['success' => false, 'error' => 'So luong san pham khong du, vui long thu lai sau!'];
                    echo json_encode($response);
                } else {
                    $oldCart = $this->session->has_userdata('Cart') ? $this->session->userdata('Cart') : null;
                    $newCart = new MCart();
    
                    $newCart->createCart(unserialize($oldCart));
    
                    $newCart->addCart($sanPham, $id);
    
                    $this->session->set_userdata('Cart', serialize($newCart));
                    $this->load->view('frontend/partial_view/cardView');
                }
    
            }
            // var_dump($newCart);
        } catch (\Exception $e) {
            $response = ['success' => false, 'error' => $e->getMessage()];
            echo json_encode($response);
        }
    }

    public function updateProduct($id, $quanlity) {
        try {
            $sanPham = $this->Mproduct->product_detail($id);
            if ($sanPham != null) {

                if($sanPham['number']-$sanPham['number_buy'] < $quanlity) {
                    $response = ['success' => false, 'error' => 'So luong san pham khong du, vui long thu lai sau!'];
                    echo json_encode($response);
                } else {
                    $oldCart = $this->session->has_userdata('Cart') ? $this->session->userdata('Cart') : null;
                    if($oldCart != null) {
                        $newCart = new MCart();
    
                        $newCart->createCart(unserialize($oldCart));
        
                        $newCart->updateCart($id, $quanlity);
        
                        $this->session->set_userdata('Cart', serialize($newCart));
                        $this->load->view('frontend/partial_view/cardView');
                    }
                    
                } 
            }
            
        } catch (\Exception $e) {
            $response = ['success' => false, 'error' => $e->getMessage()];
            echo json_encode($response);
        }
    }

    public function updateProductHeader() {
        $this->load->view('frontend/partial_view/headerCart');
    }

    public function deleteProduct($id) {
        try {
            $oldCart = $this->session->has_userdata('Cart') ? $this->session->userdata('Cart') : null;
            $newCart = new MCart();
    
            $newCart->createCart(unserialize($oldCart));
    
            $newCart->deleteCart($id);
    
            if(Count($newCart->sanPham) > 0) {
                $this->session->set_userdata('Cart', serialize($newCart));
            } else {
                $this->session->unset_userdata('Cart');
            }
            $this->load->view('frontend/partial_view/cardView');
        } catch (\Exception $e) {
            $response = ['success' => false, 'error' => $e->getMessage()];
            echo json_encode($response);
        }
    }
    
    
}