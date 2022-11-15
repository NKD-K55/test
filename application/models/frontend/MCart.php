<?php
class MCart {

    public $sanPham = null;
    public $tongGia = 0;
    public $tongSoLuong = 0;

    function __construct()
    {
        
    }

    public function createCart($cart) {
        if ($cart != null) {
            $this->sanPham = $cart->sanPham;
            $this->tongGia = $cart->tongGia;
            $this->tongSoLuong = $cart->tongSoLuong;
        }
    }

    public function addCart($sanPham, $id) {

        $price = $sanPham['price'];
        if ($sanPham['price_sale'] > 0) {
            $price = $sanPham['price_sale'];
        }

        $sanPhamInfo = ['id' => $id, 'name'=>$sanPham['name'], 'price'=>$price, 'image'=>$sanPham['avatar'], 'alias' => $sanPham['alias']];
        $newSanPham = ['quanlity' => 0, 'price' => $price, 'info'=>$sanPhamInfo];
        if ($this->sanPham) {
            if (array_key_exists($id, $this->sanPham)) {
                $newSanPham = $this->sanPham[$id];
            }
        }
        $newSanPham['quanlity']++;
        $newSanPham['price'] = $newSanPham['quanlity'] * $price;
        $this->sanPham[$id] = $newSanPham;
        $this->tongGia += $price;
        $this->tongSoLuong += 1;
    }

    public function updateCart($id, $quanlity) {
        $this->tongSoLuong -= $this->sanPham[$id]['quanlity'];
        $this->tongGia -= $this->sanPham[$id]['price'];

        $this->sanPham[$id]['quanlity'] = $quanlity;
        $this->sanPham[$id]['price'] = $quanlity * $this->sanPham[$id]['info']['price'];

        $this->tongSoLuong += $this->sanPham[$id]['quanlity'];
        $this->tongGia += $this->sanPham[$id]['price'];
    }

    public function deleteCart($id) {
        if(isset($this->sanPham[$id])) {
            $this->tongSoLuong -= $this->sanPham[$id]['quanlity'];
            $this->tongGia -= $this->sanPham[$id]['price'];
            unset($this->sanPham[$id]);
        }
    }
}